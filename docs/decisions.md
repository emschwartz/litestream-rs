# Decisions

The trade-offs this project has made, one entry each. D1 to D7 moved here from `docs/PLAN.md` section 8 and keep their ids. Entries marked **delegated** were decided by Fable (claude-fable-5-2) at the owner's request; the owner can overturn any of them.

Note: `docs/BEHAVIOR.md` section "open items" also uses ids D1 to D11 for defects. They are a different series. This file calls them "BEHAVIOR D11" and so on.

## Index

| Id | Question | Choice | Decided by | Date | Status |
|---|---|---|---|---|---|
| D1 | Own repository or a crate in the application's workspace? | Own repository | owner | by 2026-11-20 | decided |
| D2 | Track rolling checksums in Rust-written files? | Not until after cutover | owner | by 2026-11-20 | decided |
| D3 | Adopt the existing prefixes or start new ones? | Adopt | owner | by 2026-11-20 | decided |
| D4 | Local staging layout? | Go's meta-directory layout | owner | by 2026-11-20 | decided |
| D5 | Function length limit? | 70 lines, with an escape | owner | by 2026-11-20 | decided |
| D6 | How far does v1 depart from Go? | Copy Go; add nothing Go does not speak (amended by D8) | owner | 2026-11-20 | decided |
| D7 | When does replication stop? | As Go, keep trying; one default stop (amended by D12, D14) | owner | 2026-11-20 | decided |
| D8 | May the engine keep local state Go does not have? | Yes, one local file, the journal | owner; wording by Fable | 2026-11-22 | decided |
| D9 | How is the journal settled: more design rounds or a model? | A model-checked spec | owner | 2026-11-22 | decided |
| D10 | Page-digest catch-up files in v1? | No | Fable, **delegated** | 2026-11-22 | open to owner review |
| D11 | Which snapshots are dropped as unneeded? | Three cases, see entry | owner (boot rule); #108 open | 2026-11-22 | open to owner review |
| D12 | Boot finds replica files it cannot vouch for: stop or snapshot on top? | Stop, unless the WAL or a record vouches; `lsr take-over` | owner | 2026-11-22 | decided |
| D13 | Names: the command, the stop message, the record, the setting | See entry | owner | 2026-11-22 | decided |
| D14 | Default for `if-replication-stops`? | `hold-start` | owner | 2026-11-22 | decided |
| D15 | What trips fail-stop? | An unsafe stop, or unreplicated past a bound; nothing else | Fable, **delegated** | 2026-11-22 | open to owner review |
| D16 | Does a boot refusal by the store hold the app? | Yes, until the first listing succeeds | Fable, **delegated** | 2026-11-22 | open to owner review |
| D17 | Is "unreplicated too long" on by default? | No, opt-in under `stop-app` | Fable, **delegated** | 2026-11-22 | open to owner review |
| D18 | Unsafe stop while running: stop the app at once? | Yes, under `stop-app` | Fable, **delegated** | 2026-11-22 | open to owner review |
| D19 | After stopping the app under `-exec`: exit or stay up? | Exit, with its own code | Fable, **delegated** | 2026-11-22 | open to owner review |
| D20 | Can plain sidecar stop the app by holding the write lock? | Not in v1 | Fable, **delegated** | 2026-11-22 | open to owner review |
| D21 | Must the daemon be stopped for `lsr take-over`? | No; restart to apply | Fable, **delegated** | 2026-11-22 | open to owner review |
| D22 | The bound for "unreplicated too long"? | 10 minutes, minimum 5, no byte bound | Fable, **delegated** | 2026-11-22 | open to owner review |
| D23 | A delete without a directory sync: fix or argue harmless? | Fix | project rule: found defects are fixed (#116) | 2026-11-22 | decided |
| D24 | Names for store refusals and the error format | Four cause names, "{op} refused: {why}" | owner (#108) | 2026-11-22 | decided |
| D60 | Find a merged file that a lagging listing hides, whose level-0 inputs are gone? | Read its level key from the starts the listing implies; the case with no start left stays known-wrong | Claude, **delegated** (route from the parent session) | 2026-11-22 | open to owner review |
| D61 | A lagging listing hides everything above a wide hole: keep a marker object in the replica? | Yes, in the smallest form: `newest-snapshot`, written once per snapshot and believed only when a read finds the snapshot it names. No newest-transaction marker. Amends D6 and D8 | Fable, **delegated** | 2026-11-22 | open question (owner), issue #169 |

## D1. Own repository or a crate in the application's workspace

- **Options:** own repository; a crate inside the application.
- **Choice:** own repository.
- **Cost accepted:** a second repository to release and version.
- **Why:** its own CI matrix (Go binaries, fuzzing, Linux power-loss tests), no application dependency, usable by others.
- **Revisit if:** the CI matrix shrinks to what the application's CI could run.
- **Links:** `docs/PLAN.md`.

## D2. Rolling checksums in Rust-written files

- **Options:** track them now; write `NoChecksum` as Go does.
- **Choice:** not until after cutover. v1 writes `NoChecksum`.
- **Cost accepted:** no file-to-file checksum chain, so a wrong neighbour is not caught by the format.
- **Why:** Go's level compactor forces `NoChecksum` on its output while copying the last input's post-apply checksum (`compactor.go:165` at 041a8c3), so a tracked last input fails with "post-apply checksum not allowed" (read in code, not run). Go restore reads tracked chains only from v0.5.18. Tracking would break rollback to Go.
- **Revisit if:** Go is no longer the fallback writer. With the page checksum table, tracking needs no prior page content and no extra scan, so rollback is the only reason to wait.
- **Links:** `docs/PLAN.md`.

## D3. Adopt the existing prefixes or start new ones

- **Options:** adopt; start new prefixes.
- **Choice:** adopt.
- **Cost accepted:** the port inherits whatever Go left on the replica.
- **Why:** tooling, the VFS and the retention window carry over.
- **Revisit if:** inherited replica state proves unsound (see `snapshot-necessity` D4, not decided).
- **Links:** `docs/PLAN.md`.

## D4. Local staging layout

- **Options:** Go's meta-directory layout; a new layout.
- **Choice:** Go's layout.
- **Cost accepted:** the layout cannot change while Go is a fallback.
- **Why:** the requirement is that switching between Go and Rust in either direction, at any moment, is a restart (phase 3 alternation test).
- **Revisit if:** Go is no longer the fallback writer.
- **Links:** `docs/PLAN.md`.

## D5. Function length limit

- **Options:** no limit; 70 lines (TigerStyle).
- **Choice:** start at 70 lines, with an `#[expect(…, reason)]` escape and a checkpoint at phase 1 exit.
- **Cost accepted:** some functions are split for the limit alone.
- **Revisit if:** the escapes pile up.
- **Links:** `docs/PLAN.md`.

## D6. How far v1 departs from Go

- **Options:** copy Go exactly; copy Go unless a better behaviour is obvious; redesign.
- **Choice (owner, 2026-11-20):** v1 copies Go's behaviour unless a better behaviour is obvious, and adds no file, header, metadata or object Go does not speak. Correctness checks that read more (verify before use, gap errors, the compare of the whole newest staged file, the boot read-back) stay, since they change nothing Go sees.
- **Cost accepted:** improvements that need new state wait in `docs/proposals/beyond-go.md` and its issues.
- **Revisit if:** a defect cannot be fixed without new state. This happened: see D8.
- **Links:** `docs/PLAN.md`, `docs/proposals/beyond-go.md`.

## D7. When replication stops

- **Options:** stop on persistent failure; keep trying as Go does.
- **Choice (owner, 2026-11-20):** as Go, keep trying. Every persistent failure retries with Go's capped backoff (5 minutes) and shows as a typed degraded state in `health()` and events, scoped to the task that failed. The one default stop is another writer on the prefix: uploads and maintenance stop, since continuing would interleave two databases into one history and delete the other writer's files. Each cause's response (keep trying or stop) is a config option.
- **Cost accepted:** an application can run for a long time with replication degraded.
- **Amended by:** D12 (the other-writer stop now also applies at boot) and D14 to D18 (what a stop does to the application). The retry behaviour itself is unchanged.
- **Links:** `docs/PLAN.md` phase 3 task 10, `crates/core/src/fault.rs` (`Responses::GO_LIKE`).

## D8. Local engine state Go does not have (amends D6)

- **Question:** D6 forbids any file Go does not speak. Several defects (a forgotten WAL sync failure, a forgotten stop, crash-loop snapshots) need a fact that survives the process.
- **Options:** keep D6 as written and accept the defects; allow new replica objects; allow one local file.
- **Choice:** one local file per database, `<meta>/lsr-journal`. Proposed D6 wording: v1 adds no file, header, metadata or object that Go reads or that reaches the replica. The journal may make the engine refuse, wait, snapshot or checkpoint, and may vouch for a staged file this engine wrote. It never makes the engine write a replica object or staged file Go could not have written.
- **Cost accepted:** one `fdatasync` per upload tick. A second thing to keep correct across versions.
- **Revisit if:** Go starts removing unknown files from the meta directory, or a fact is needed that must reach the replica.
- **Status note:** the journal's storage contract is on main; the D6 wording above is from design v5 and is not yet in `docs/PLAN.md`.
- **Links:** `docs/journal.md`, #109, #114.

## D9. The journal as a model-checked spec

- **Options:** a sixth design round; build an exhaustive model and write the contract from it.
- **Choice:** the model. `docs/journal.md` is the contract, amended where the model found the prose wrong. The model (`lsr_spec::journal`) shares no code with the implementation.
- **Cost accepted:** a spec crate to maintain; the model covers storage only, not what the engine does with the facts.
- **Revisit if:** the implementation and the model drift, or a defect appears that the model's world cannot express.
- **Links:** `docs/journal.md`, `crates/spec/src/journal/`, #109, #114.

## D10. Page-digest catch-up files

- **Question:** keep a per-page digest of the replica's tip (about 3 MiB per GiB) so that doubt costs a local read and a small catch-up file, not a snapshot?
- **Options:** build it in v1; not now.
- **Choice:** not in v1.
- **Cost accepted:** every real doubt still costs a full snapshot upload.
- **Why:** a wrong table entry leaves a stale page on the replica silently. The only detector is a scheduled read of the whole replica, and a restore inside that window returns a corrupt database with no error.
- **Revisit if, all four:** doubt snapshots dominate upload bytes after the budget and the cheaper savings; the table is replaced whole with a checksum and bound to the journal's floor; it is the existing `PageChecksumTable`, persisted and scrubbed; a wrong entry fails loudly at restore.
- **Links:** `docs/journal.md`, design v5 on `claude/analysis-notes`.

## D11. Snapshots dropped as unneeded

- **Question:** where does the engine snapshot when a cheaper sound proof exists?
- **Choice, three cases:**
  - A replica ahead of staging at boot continues with no snapshot when the WAL vouches for the replica's newest file (part of D12).
  - At 4096 pending tokens the sync step holds its position; it no longer stages a file that is certain to be refused and replaced by a snapshot (#108).
  - After boot drops an unreadable staged file, the engine stages again from the WAL when the WAL still holds the range (#108).
- **Cost accepted:** one extra GET at boot in the first case.
- **Revisit if:** the liveness oracle still shows snapshots with no real doubt behind them.
- **Links:** #108 (merged), D12.

## D12. Boot finds replica files it cannot vouch for

- **Question:** at boot the replica holds files above this database's position. They may be ours (staging wiped, a VM put back, a Go run) or another writer's. With no local record, which?
- **Options:** (a) stop unless the WAL vouches; (c) snapshot on top, as Go does; a new generation or prefix.
- **Choice (owner):** (a). Every replica id above the local position must be vouched for by the WAL (continue, no snapshot) or by a record (snapshot above). Otherwise the engine stops with `OtherWriter`. `lsr take-over <db>` writes the record. `lsr restore` writes it too, so a restore followed by a start never stops.
- **Cost accepted:** a false stop when the proof is gone: the meta directory deleted after a WAL restart, a VM or volume put back from an older copy, a Go run with another meta directory followed by a WAL restart. All are operator actions, and the fix is one command. Go never stops here, so this departs from Go; `ContinueAndInterleaveHistories` keeps Go's behaviour.
- **Why:** in normal operation the replica is never ahead, so only damage, an operator or another writer reaches this case. Snapshotting on top lets an empty or foreign database become "latest", and retention then removes the real one.
- **Revisit if:** false stops show up in unattended flows, or conditional writes (#96) plus a durable stop make "last to boot wins" safe enough.
- **Links:** `docs/BEHAVIOR.md` BOOT-6, BOOT-22, WORLD-3; #110; `crates/core/src/actor.rs` (`boot_recover`), `recover.rs` (`recover_traced`).

## D13. Names

- **Choice (owner):**
  - the command: `lsr take-over <db>`;
  - the stop message: "replicate refused: the replica holds transactions up to {txid} that this database has no record of writing. Another replicator may be using this prefix. To replicate this database over them: `lsr take-over <db>`. To get them instead: `lsr restore`.";
  - the record: "takeover record";
  - the setting: `if-replication-stops`, values `report`, `hold-start`, `stop-app`.
- **Options considered:** `adopt` and `claim` for the command; `on-unsafe-stop` for the key.
- **Why:** "adopt" reads as pulling the replica's data in, which is `restore`. Go's `reset` clears local state only, so that word was avoided.
- **Cost accepted:** none known.
- **Revisit if:** operators misread any of them in practice.
- **Links:** #115.

## D14. `hold-start` is the default

- **Options:** `report` (503 and metrics only, as Go); `hold-start`; `stop-app`.
- **Choice (owner):** `hold-start`. An unsafe stop at boot does not start the `-exec` child, and the library refuses writes.
- **Cost accepted:** under `-exec` and the library, a false stop at boot (D12) delays the application's start until an operator acts. Go starts the child regardless.
- **Revisit if:** boot holds happen with no operator action behind them.
- **Links:** #115, D7.

## D15. What trips fail-stop (delegated)

- **Options:** a failure count; any degraded state; a stop only; a stop plus a bound on data at risk.
- **Choice:** two triggers and a boot gate. (1) The engine is stopped for an unsafe cause. (2) The oldest unreplicated commit is older than the bound, and a probe made at the bound also failed. (3) D16. No failure count, degraded flag or stalled task trips anything.
- **Cost accepted:** a task that is degraded while uploads still land is invisible to fail-stop; it shows in health only.
- **Why:** blips, throttling and maintenance faults then cannot take the application down. The clock comes from the oldest pending staged file, so a crash loop cannot reset it.
- **Revisit if:** a fault is found that puts data at risk without growing the unreplicated age.
- **Links:** #115, `failstop-design.md` (scratchpad, to be filed).

## D16. A boot refusal by the store holds the app (delegated)

- **Question:** before any listing has succeeded, the store refuses this process (`credentials`, `permission_denied`, `clock_skew`, `bucket_missing`, `tls`). Hold the application?
- **Options:** start anyway and report; hold until the first listing succeeds; hold on any unreachable store.
- **Choice:** hold on a refusal, under `hold-start` and `stop-app`. Start on timeouts, 5xx and throttling. The hold releases by itself at the first successful listing.
- **Cost accepted:** a restart during an identity-service outage, or with expired keys or an expired certificate, keeps the application down until it is fixed.
- **Why:** a refusal before any success means nothing shows this configuration ever worked. A wrong bucket or key at deploy is the usual way an application runs for weeks with no backup. A hold that releases by itself costs a delayed start, not a page.
- **Revisit if:** boot refusals turn out to be mostly transient in real deployments.
- **Links:** #108 (`Denied` in `crates/core/src/store.rs`), #115.

## D17. "Unreplicated too long" is opt-in (delegated)

- **Options:** on by default; opt-in under `stop-app`.
- **Choice:** opt-in. The default never stops a running application.
- **Cost accepted:** with the default, an application can keep writing through a long outage with nothing replicated; only health says so.
- **Why:** a 4 hour store outage carries about a 2 in 100,000 chance of also losing the local disk. Stopping the application makes the outage certain. That suits only an operator whose loss cost is far above their downtime cost, and they can say so.
- **Revisit if:** a user loses data this way, or the owner wants durability over availability by default.
- **Links:** #115, D7.

## D18. An unsafe stop while running stops the app at once (delegated)

- **Options:** at once; only when a commit is actually unreplicated.
- **Choice:** at once, under `stop-app` only.
- **Cost accepted:** a wrong other-writer stop takes the application down with no grace.
- **Why:** a stop is terminal. Waiting for the first unreplicated commit delays the same outcome and adds a state to test.
- **Revisit if:** mid-run stops prove to have false positives.
- **Links:** #115.

## D19. After stopping the app under `-exec`, lsr exits (delegated)

- **Options:** exit; stay up and restart the child on recovery.
- **Choice:** SIGTERM, the existing grace, SIGKILL, one last drain attempt, then exit with a code of its own. The next boot holds the child until the cause clears.
- **Cost accepted:** recovery needs the supervisor to restart lsr.
- **Why:** it matches Go's model, where the child's end ends the run, and leaves restarts to the supervisor.
- **Revisit if:** the exit code convention changes (#41).
- **Links:** #115, #41, `crates/sidecar/src/exec.rs`.

## D20. No write-lock enforcement for plain sidecar (delegated)

- **Options:** hold SQLite's write lock so the application's writes fail; do nothing beyond 503.
- **Choice:** not in v1. Plain sidecar stays up and answers 503. `stop-app` without `-exec` is refused at config check.
- **Cost accepted:** plain sidecar cannot stop an application.
- **Why:** the application would see "database is locked", which sends whoever debugs it the wrong way.
- **Revisit if:** plain-sidecar users ask for enforcement.
- **Links:** #115.

## D21. `lsr take-over` beside a running daemon (delegated)

- **Options:** require the daemon stopped; go through the daemon; write a file the daemon reads at boot.
- **Choice:** the takeover record is its own file in the meta directory, written by rename and read at boot only. The command works while a daemon runs or holds the app; the daemon must be restarted to act on it.
- **Cost accepted:** one more small file until the journal can hold it; a restart to apply.
- **Why:** a held `-exec` daemon is the case where the operator needs the command most, and stopping it first may end the container.
- **Revisit if:** the journal gains a safe second writer, or the command moves into the daemon.
- **Links:** #110 (stop record, meta-directory lock), `docs/journal.md`.

## D22. The bound: 10 minutes, no byte bound (delegated)

- **Options:** 2 minutes (when a task is marked stalled); 5; 10; an hour; an added byte bound.
- **Choice:** `max-unreplicated-age` defaults to 10 minutes, minimum 5. No byte bound in v1.
- **Cost accepted:** up to 10 minutes of commits at risk before `stop-app` acts (about 60,000 at 100 commits a second).
- **Why:** retries land at 255 s and 511 s, so 10 minutes clears blips and short incidents and still leaves two retries. 5 minutes is the backoff cap. A full staging disk already shows as a fault that grows the age.
- **Revisit if:** real incident data disagrees. The outage figures were from memory.
- **Links:** #115.

## D23. A delete without a directory sync: fixed (BEHAVIOR D11)

- **Options:** argue that a staged file returning after power loss is harmless; sync the directory after a delete.
- **Choice:** fix it.
- **Cost accepted:** one directory fsync per delete call. Measured: a single-key delete goes from 68 to 87 µs to 213 to 265 µs. Deletes are batched, so at most one fsync per prune.
- **Revisit if:** delete cost shows up in a profile.
- **Links:** #116 (open), `docs/BEHAVIOR.md`.

## D24. Names for store refusals

- **Choice:** health and metric cause names `permission_denied`, `clock_skew`, `bucket_missing`, `tls`; `credentials` unchanged. Error text "{op} refused: {why}", which replaces "not authorized".
- **Options considered:** one `credentials` class for all, as before.
- **Why:** clock skew, a missing bucket and a failed certificate check each sent operators to rotate keys that were fine.
- **Cost accepted:** four more causes to keep classified correctly per store.
- **Revisit if:** a store returns a refusal that fits none of them.
- **Links:** #108 (merged).

## D60. Reading higher-level keys past a lagging listing

- **The case:** another writer's level-0 file is merged into a level-1 file, and an operator then removes the level-0 file. A listing that lags hides the level-1 file. Before this change, the read past the listing named only level-0 keys and snapshots, so it could not find the level-1 file. The engine took the replica as ending below it and uploaded its own next transaction into the freed key: one transaction, two contents (seeds 0x4dc and 0x517).
- **Options:** (a) for each transaction the probe reads, also read the level 1 to 8 keys that end at it, from each start the listing implies; (b) change the simulator so that the operator removes every file holding the transaction, or call a partial cleanup outside the contract; (c) accept it as known-wrong.
- **Choice:** (a). The starts for level `k` are the transaction after level `k`'s newest listed file and the start of the first level `k - 1` file above it. A compaction into level `k` begins at one of them, ours and Go's alike. (b) was ruled out by the parent session: a partial cleanup is a plausible operator mistake, and the simulator must not get kinder.
- **Cost accepted:** one more one-byte read per start for each probed transaction. A fresh boot listing with level 1 and 2 in use goes from 12 to 20 reads, at most 76 with every level in use. Steady state is unchanged: the probe runs only after a listing.
- **Residue:** a lag that also hides level `k`'s previous file hides the start too, and the file stays unnamed (BOOT-25 in `docs/BEHAVIOR.md`, known wrong). It needs two compactions of one level inside the lag window and level-0 inputs removed by someone other than this engine.
- **Revisit if:** a real store shows lag windows long enough for two compactions of one level, or a writer identity on the replica makes the second writer's files distinguishable.
- **Links:** issue #99 (the item from #96), #96, `crates/core/src/probe.rs`.

## D61. A marker object for what a listing cannot name (amends D6, D8)

- **Question:** a restore of `Latest` tells a lagging listing from a whole one by reading keys it can name past the listing's newest transaction. After a bucket rollback the replica's next file is a snapshot `1-N` far above the hole, which no reader can name, so a listing that lags returns the old state with success (sim seed 0x7f8 on `claude/overlapping-faults`: listing ends at 0x21, snapshot `1-8c`, newest 0x9b).
- **Options:**
  - A. The bounded walk from t+1 alone, with the wide hole recorded as known wrong.
  - B1. A fixed-name object holding the newest transaction, overwritten per upload or per maintenance pass; a listing that ends below it fails.
  - B2. A fixed-name object holding the newest snapshot's key, overwritten once per snapshot; the reader reads the snapshot it names.
  - C. A signal already in the format.
  - D. Only the engine fix (stop uploading above a hole).
- **Choice:** B2, on top of A and D.
  - **Name:** the object is `<prefix>/newest-snapshot` (file store: beside `ltx/`), called the "snapshot pointer" in prose. Its body is one line, the snapshot's file name (`0000000000000001-000000000000008c.ltx`), parsed by the strict file-name parser. Anything else reads as no pointer.
  - **Writer:** before uploading any snapshot, the engine puts the pointer naming it and waits for that put to be confirmed. An idle database writes none. A pointer whose snapshot never arrives (a crash) is harmless, see the reader.
  - **Reader:** `probe.rs` adds one key to what it names. When the pointer names a snapshot ending above the listing's newest, the reader reads one byte of that snapshot. Found means "list again", counted as today (three lagging listings fail with `ListingBehind`). Not found, no pointer, or an unreadable pointer means nothing, and the walk proceeds as without it. The restore reports which of these it saw. The uploader's boot and maintenance get the same key through `probe.rs`.
  - **Rule kept:** only a read that finds an LTX file is evidence. The pointer makes a key nameable and decides nothing by itself, so a stale, missing, orphaned or corrupt pointer gives a weaker check and never a wrong state or a refusal.
  - **Rejected, B1:** it lets an object nothing verifies refuse a restore. It refuses forever when it sits above the replica (a Go writer restarted on a prefix whose level directories were removed). It needs level-0 retention tied to it to be complete. Per upload it doubles the PUT count (86,400 a day at Go's 1 s sync interval).
  - **Nothing relies on the pointer until the Go differential below is in the gate.** Until then WORLD-4's wide-hole case stays "known wrong".
- **Closes:** a lag that hides a snapshot and everything after it, however wide the hole below, when this engine wrote the snapshot and the pointer read is current. That is seed 0x7f8 once the engine fix lands: the pointer names `1-8c`, the read finds it, the restore fails `ListingBehind`.
- **Does not close:**
  - files above a hole with no snapshot above them (the engine before its fix, an operator's delete, anything Go wrote);
  - a level 1 to 8 file whose inputs are gone and whose start the listing hides (the residue row on `claude/merged-level-probe`);
  - a replica that lost its newest files and lists whole (BOOT-19: needs a newest-transaction record, B1's job, not decided here);
  - a stale pointer read;
  - a failed-over copy that is older as a whole (WORLD-40).
- **Cost accepted:**
  - D6 and D8 said no object reaches the replica that Go does not speak. This is one. D8's "revisit if a fact must reach the replica" is met, since a restore runs where no journal exists. The owner decided D6, so this amendment needs the owner's word before code lands.
  - One PUT per snapshot (R2 Class A is $4.50 per million; default cadence is daily) and one GET per restore attempt and per boot.
  - A snapshot waits on the pointer's put. A credential scoped to the level directories only would stall snapshots, reported as a typed degraded state (D7).
  - Under a Go writer the pointer goes stale and the check falls back to the walk.
- **Why:**
  - **No cheaper signal exists (read in `ltx` v0.5.3 and litestream 041a8c3).** The LTX header's only cross-file field is `PreApplyChecksum`, which points backward and is zero under D2. `Timestamp` is a write time. The trailer holds two checksums. Go's one metadata key is the file's own timestamp. A lagging listing paginates as a whole older view. A HEAD needs a name, and `1-N` has none, because snapshots follow the clock and losses, not a count.
  - **Go tolerates the object (read in code, not run).** Go lists only `<path>/%04x/` per level and `<path>/generations/` (`s3/replica_client.go:1588`, `:1371`). Level iterators skip names failing `^[0-9a-f]{16}-[0-9a-f]{16}\.ltx$` (`ltx.go:501`; `s3/replica_client.go:1730`; `file/replica_client.go:115`). Retention deletes only `FileInfo`s those iterators returned (`compactor.go:238-400`, `db.go:3169`, `:3292`). `DeleteAll` lists the whole prefix but is called only from `internal/testingutil`. Go already puts a fixed-name object at the same place, `<path>/lock.json` (`s3/leaser.go:23`, not wired to a command at this commit). The root is chosen over a level directory because there Go would HEAD the object on every metadata listing. Go's NATS client errors on a name it cannot parse (`nats/replica_client.go:274`); this port has no NATS store.
  - **Consistency.** Cloudflare R2 documents read-after-write, overwrite and listing as strongly consistent, so neither the lag nor a stale pointer arises there (custom domains with caching excepted). Tigris documents strong consistency for same-region requests and for Multi-region and Single-region buckets, and eventual consistency for cross-region reads of Global and Dual-region buckets ("a cached or on-demand copy", lag "typically sub-second"). There the listing and an overwritten pointer can both be stale at once, so the pointer is at best the weaker check. Because it is overwritten about daily and confirmed by reading a never-overwritten key, a stale read names the previous snapshot and falls through to the walk.
  - **Not verified:** the AWS consistency section (the page was reached, the section was not returned; the December 2020 statement is from memory); AWS and Tigris request prices; whether Tigris has a per-request strong-read option; whether a Tigris cross-region read of a never-read key is exact, which every probe here already assumes.
- **The Go differential that must exist first** (lsr-compat; S3 and file stores; Go v0.5.16 and the newest pinned; pages compared), with `newest-snapshot` at the prefix root:
  1. Go `restore` gives the same pages and exit code as without it, for latest, `-txid` and `-timestamp`.
  2. Go `replicate` adopts the prefix, uploads, compacts every level, takes a snapshot and runs snapshot and level-0 retention through a full cycle with no error, and the object is still there, byte for byte.
  3. Go's `ltx` listing is unchanged.
  4. With the pointer stale (Go snapshotted after it) and with it naming a snapshot Go's retention removed, Rust restore equals Go's.
  5. The alternation test passes with the pointer present, SIGKILL between the pointer's put and the snapshot's included.
  - Simulator: a law where the pointer's read is stale or absent while listings lag, judged with no allowance (the answer is the replica's or an error); a plant that drops the pointer check.
- **`docs/BEHAVIOR.md` once it lands:**
  - WORLD-4 (the restore read-past row): the reads gain the pointer and the snapshot it names; its "lag that hides files past a real hole" limit narrows to the list under "Does not close".
  - BOOT-18 and MAINT-12: the same key joins the read-past list.
  - A6 and the environment row "Listings that lag the bucket": gain "an overwritten object read stale: modelled, weaker check".
  - BOOT-19 and the level 1 to 8 residue row: unchanged.
  - New rows: no pointer (a Go-written replica); pointer unreadable; pointer names a snapshot no read finds; the pointer's put refused.
- **Revisit if:**
  - Go starts listing or cleaning the prefix root, or ships a `newest-snapshot` of its own.
  - The Tigris soak shows overwritten reads stale for longer than listings.
  - The engine can still upload above a hole without a snapshot after its fix, or the owner wants a restore to refuse a replica that lost its newest files. Either reopens B1.
- **Status:** the owner keeps this as an open question, tracked in issue #169. Nothing is built.
- **Links:** issue #169; `crates/core/src/probe.rs` and `crates/core/src/restore.rs` (`listed_past`) on `claude/merged-level-probe`; seed 0x7f8 on `claude/overlapping-faults`; D2, D6, D8, D33, D34; https://developers.cloudflare.com/r2/reference/consistency/; https://www.tigrisdata.com/docs/concepts/consistency/.

## Unverified, not recorded

- Who decided D1 to D5 and when: `docs/PLAN.md` says "Decided" with no name or date. Listed as owner, by 2026-11-20.
- D8's exact wording: the owner approved adding the journal; the wording of the D6 amendment is Fable's.
- `snapshot-necessity` owner decisions D1 to D5 there (the price of an adopted journal, the catch-up file's failure class, download-and-diff, whether a handover heals, new cause names) and savings S2, S3, S5, S6, S7: no decision found.
