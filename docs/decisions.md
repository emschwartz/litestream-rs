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
| D25 | A regression the driver excused (a rollback) that the engine never reports: fail the run? | No; count it (`history.excused.unreported`) | Claude (checker-holes), **delegated** | 2026-11-22 | open to owner review |
| D26 | Which engine events count as reporting a loss of history? | Loss-naming snapshot causes, a first sync over a replica holding files, and the stops and degraded states of a regressed replica or WAL | Claude (checker-holes), **delegated** | 2026-11-22 | open to owner review |
| D27 | Keep a stricter oracle check when it finds a known engine defect? | Yes; pin the seed as an ignored expected-fail test | Claude (checker-holes), **delegated** | 2026-11-22 | open to owner review |
| D28 | How does the simulator's clock move inside a tick? | A frozen law per run, drawn last in the swarm mix; the default mix keeps one reading per tick | Claude (checker-holes), **delegated** | 2026-11-22 | open to owner review |
| D29 | Where do the behaviour table's rows live? | `docs/behavior.toml`, rendered into `docs/BEHAVIOR.md` | parent session's request; layout by Claude (checker-holes) | 2026-11-22 | decided |
| D30 | How is Toxiproxy provided to the tests? | Built from source with `go install` at a pinned version | Claude (checker-holes), **delegated** | 2026-11-22 | open to owner review |
| D31 | Must a PR re-merge main and re-run CI before it lands? | No while churn is high: merge on the PR's own green; main's CI integrates; fix forward | owner | 2026-11-22 | decided |
| D32 | How is an unusable KMS key reported? | As a permission refusal, `permission_denied` | Claude, **delegated** | 2026-11-22 | open to owner review |
| D33 | Narrow the Go drop-in surface to shrink unverified code? | No: full drop-in; bring config, CLI and sidecar under behaviour rows and Go differentials | owner | 2026-11-22 | decided |
| D34 | A restore of the latest state that a writer keeps outpacing: fail or answer? | Answer the last listing, flagged | parent session (#126) | 2026-11-22 | open to owner review |
| D35 | When may the simulator accept a task still stalled at the end of a run? | Only while the world still imposes the stall's fault; the excuse table is empty today | Claude (stall-liveness), **delegated** | 2026-11-22 | open to owner review |
| D100 | How does the journal hold the unproven-WAL debt, and when is it written? | Written at the end of the tick that changes it; a new failure may raise it at any step; `JournalUnwritable` while the journal lags | Claude (journal), **delegated** | 2026-11-22 | open to owner review |
| D130 | How does the simulator run the real directory store, and who judges it? | A filesystem trait in store-fs, an in-memory filesystem under the crash law in the sim, half the swarm on it, a staging-contract judge | Claude (sim-real-store-fs), **delegated** | 2026-11-22 | open to owner review |

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

## D25. An excused regression the engine never reports

- **Options:** fail the run when the driver's excuse for a `Latest` that went back (a bucket rollback, an operator's delete) is never followed by the engine's own report of a loss; count it.
- **Choice:** count it in `history.excused.unreported` and report it, not fail. A rewind the engine did not report does fail (`Anomaly::Unsignaled`).
- **Why:** the engine raises `RemoteRegressed` only on an uploader listing (BEHAVIOR D17), so about 860 of 3000 seeds' rollbacks go unreported; failing would make every simulator run red on a known open defect.
- **Cost accepted:** an engine that never notices a rolled-back bucket shows only in a count.
- **Revisit if:** D17 is fixed: then make the count fatal.
- **Links:** #111, `crates/history/src/judge.rs`.

## D26. What counts as the engine reporting a loss

- **Options:** any snapshot; only `LocalRewound`; the causes that say the history went back or cannot be followed.
- **Choice:** `BehindRemote`, `NewestStagedFileUnreadable`, `WalMissing`, `GenerationChanged`, `PrevFrameMismatch`, `LastPageMismatch`, `LocalRewound`, `StagedUnverified`, `WalSyncFailed`; `FirstSync` only over a replica that already holds files; stops and degraded states `RemoteRegressed`, `OtherWriter`, `WalCorrupt`, `WalTruncated`, `WalUnlinked`, `WalSyncFailed`. Not a stall.
- **Why:** each of these tells an operator the replicated history may not continue; `FirstSync` over files is the engine starting over with nothing to prove continuity (seed 0x6cd).
- **Cost accepted:** a cause on the list excuses any rewind after it, including one it did not cause.
- **Revisit if:** a run shows a cause on the list excusing a regression it could not explain.
- **Links:** #111, `snapshot_reports_loss` in `crates/sim/src/run.rs`.

## D27. A stricter check that finds a known defect

- **Options:** keep the old run-wide exemption until the engine is fixed; scope the exemption and keep the check strict, pinning the failing seed.
- **Choice:** strict, with seed 0xb6b as an ignored expected-fail test beside BOOT-19's.
- **Why:** an exemption that hides a real id reuse is the checker hole this work closes.
- **Cost accepted:** the nightly simulator can fail about once per 3000 seeds on that defect until conditional level-0 writes land.
- **Revisit if:** #96 lands: turn the test on.
- **Links:** #111, #96.

## D28. The simulator's clock inside a tick

- **Options:** one reading per tick (as before); every read later than the last, always; a law drawn per run.
- **Choice:** a frozen `ClockLaw` per run, drawn last in the swarm mix (half each); the default mix and every recorded history keep one reading per tick.
- **Why:** recorded seeds and planted-bug kills must keep reproducing; new behaviour is reached by the swarm.
- **Cost accepted:** only half the swarm seeds exercise clock-ordering bugs.
- **Revisit if:** a clock-ordering defect escapes the swarm.
- **Links:** #111, `crates/sim/src/clock.rs`.

## D29. The behaviour table as data

- **Options:** keep `docs/BEHAVIOR.md` hand-written; rows in TOML, the document rendered.
- **Choice:** rows in `docs/behavior.toml` (each cell verbatim), prose in `docs/behavior/template.md`, `just behavior-doc` renders, `just check` fails on drift and on citations that resolve to nothing. One file and one checker: the separate evidence-rows file this branch first added (`docs/evidence.toml`, with its own schema) was folded in before any other branch had to port rows into both. Its kill rule survives as a backing rule on `plant::` citations: the row must also cite a test the ledger entry runs, and one such test must not be ignored.
- **Why:** hand-typed citations had rotted (five resolved to nothing when checked).
- **Cost accepted:** every branch that edits section 3 edits the TOML, and open branches written against the old file need their rows moved. The fold lost the typed `[[row.kill]]` pairing (which test kills which plant): the backing rule matches by the ledger's package and filters instead, which is looser when a row cites several tests of one package.
- **Revisit if:** the marks become derived from rig records (BEHAVIOR section 7.3, item 2).
- **Links:** #111.

## D30. How Toxiproxy reaches the tests

- **Options:** its container image; a release binary download; `go install` at a pinned version.
- **Choice:** `go install github.com/Shopify/toxiproxy/v2/cmd/server@v2.12.0` into a cache (`scripts/toxiproxy.sh`). MIT licence; a test tool, never a cargo dependency.
- **Why:** the image's blob host is not reachable from every sandbox, and the repository already builds pinned Go tools.
- **Cost accepted:** a first run builds it (about a minute) and CI's `test-s3` job needs Go.
- **Revisit if:** the build breaks on a Go release.
- **Links:** #111.
## D31. Merging while many branches are in flight

- **Options:** every PR merges the latest main and re-runs CI before it lands; or a PR lands on its own green CI when it has no textual conflict, and main's CI checks the combination.
- **Choice:** land on the PR's own green. Main's CI on each merge commit is the integration test. A red main stops new merges until it is fixed forward by the session that owns the breaking merge. A PR that renames shared APIs still re-tests against main first.
- **Cost accepted:** main can be red for about one CI run after a bad combination, and not every commit on main passed CI as a whole.
- **Why:** with six or more branches open, each merge cost every other branch a merge and a 20 minute CI run, for hours of serial waiting. The one real catch in that period (renamed causes) would have shown as a red main within one run.
- **Revisit if:** before any release, or when main goes red more than once a day.
- **Links:** the merge protocol in the session notes; AGENTS.md.

## D32. An unusable KMS key is a permission refusal

- **Question:** S3 answers a request whose KMS key is disabled, missing, in the wrong state or of the wrong kind with a 400 and a `KMS.*` code (`KMS.DisabledException`, `KMS.InvalidKeyUsageException`, `KMS.KMSInvalidStateException`, `KMS.NotFoundException`, from S3's published error list). How is it reported?
- **Choice:** `Denied::Permission`, health name `permission_denied`, the same kind as a KMS key policy that refuses (403 `AccessDenied`).
- **Options considered:** a refused request (`store_refused`, what the status alone gave); a kind of its own (`kms_key`), which needs a new user-visible name.
- **Why:** an operator fixes it where they fix a permission: the key's state or policy, not the request. No new name is needed.
- **Cost accepted:** the health name does not say "KMS"; the log's error text and the store's message do.
- **Revisit if:** operators need to tell a KMS key from a bucket policy at a glance.
- **Links:** `S3Code::KmsKeyUnusable` in `crates/store-s3/src/classify.rs`.

## D33. Full drop-in replacement for Go

- **Options:** narrow the drop-in surface (refuse `dir:` scanning, AWS shared credential files and yaml.v2 quirks) to remove about 900 lines of code no behaviour row covers; or keep full drop-in and bring that code under evidence.
- **Choice:** full drop-in. The config, CLI and sidecar surface gets behaviour rows and generated differential tests against Go Litestream, with `dir:` scanning and credential resolution first, since they decide what is replicated and with which credentials.
- **Cost accepted:** about 9,200 production lines stay, and need differential harnesses against Go.
- **Revisit if:** a Go behaviour on this surface would lose data. Then ours differs, with its own entry here.
- **Links:** #121 (cuts 2, 4 and 5 withdrawn), docs/release-bar.md.

## D35. A stall at the end of a run

- **Options:** accept any stall as a degraded state (before); fail any stall once every injected fault has healed; fail it unless the world still imposes its fault.
- **Choice:** at the end of the final drain and after idle's settle window, a task still stalled fails the run (oracle `StuckRetrying`) unless an exhaustive table says the world still imposes that stall's fault. The table has no entry today. The run's end also heals the SQLite source failures still queued.
- **Why:** an engine stuck retrying a fault nothing causes any more, or reporting degraded for nothing, passed before.
- **Cost accepted:** a fault the world keeps imposing after the heal (none today) needs a table entry, or its runs fail.
- **Revisit if:** a fault is added that the end of the run cannot heal.
- **Links:** the stall-liveness PR; #104.

## D100. The unproven-WAL debt in the engine journal

- **Question:** a failed WAL fsync must survive a restart (open defect D2). When is it written, what may follow what, and what is reported when it cannot be written?
- **Choice:** the journal's debt is a function of the engine's state, written at the end of every tick in which it differs from the last record made durable (or read at boot); boot restores a recorded debt and never forgives one. The debt is paid only through its cure (TRUNCATE owed, then snapshot owed, then none), and a new failure may raise it to TRUNCATE owed at any step: the writer refuses, as a bug, a record that forgives it. A failed write is tried again every tick; `JournalUnwritable` (approved name) is reported while the journal lags the engine and clears once nothing is left to record.
- **Options considered:** write the debt inside `Actor::fault` (it has no file lent); write it before the next sync step (one more place to remember); the design's strict order No, Truncate, Snapshot, No (refuses a second failure during the paying snapshot).
- **Cost accepted:** the record lags the failure by the rest of one tick, so a kill in that window forgets it, as without a journal. One `fdatasync` (two on the first write, which creates the file) per change of the debt; none otherwise, so a database that never fails a WAL fsync never creates the journal.
- **Revisit if:** the window inside the tick is shown to matter, or later facts need ordering against the debt within a tick.
- **Links:** `crates/core/src/actor/unproven.rs`, `docs/journal.md`, `docs/BEHAVIOR.md` RUN-3 and D2.

## D130. The real directory store in the simulator (delegated)

- **Options:** keep `MemStore` with its power-loss bookkeeping as the only staging double; run `FsStore` on a temporary directory of the host (no crash law, no determinism); a filesystem trait in `lsr-store-fs` with an in-memory filesystem in `lsr-sim`.
- **Choice:** the trait (`Fs`, `FsFile`, `RealFs`; `FsStore<F: Fs = RealFs>`, so every existing user is unchanged) and `MemFs`. A file keeps its synced and current images under the block law the WAL uses; a directory entry changed since its directory's last sync follows the one directory-entry rule of `crash_law.rs` (`entry_kept`): under the block law each such entry keeps its change or goes back to what it durably held, on its own coin; under all-or-nothing a deletion stays done and any other change is undone. Half of the swarm mixes (drawn last, off in the default mix) put staging on `FsStore<MemFs>`. A judge in the runner fails a run when a staged file whose commit or delete returned success changed across a power loss (`Violation::StagedReverted`).
- **Why:** `MemStore`'s power loss assumes the store syncs what it should; the three durability plants in `store-fs` could only be killed by their named crash-replay tests. With the store itself under the crash law the swarm finds all three unaided.
- **Cost accepted:** the judge checks the store's contract, not the engine's behaviour, so it finds a store defect the engine would survive (the delete plant). Per-entry independence can split a rename (both names, or neither), which ext4 would not; it is the rule the WAL's fault VFS and `Volatile` already use, so the disk stays one law (#121). A fault the fault layer injects after publishing (`ErrorAfterPublish`) does not unsync anything on `MemFs`: the store did sync, so in this profile such a file always survives.
- **Revisit if:** the shared directory-entry rule changes; or `MemFs` and the host disagree in `differential::the_doubles_answer_as_the_real_store_does`.
- **Links:** #127 item I.

## Unverified, not recorded

- Who decided D1 to D5 and when: `docs/PLAN.md` says "Decided" with no name or date. Listed as owner, by 2026-11-20.
- D8's exact wording: the owner approved adding the journal; the wording of the D6 amendment is Fable's.
- `snapshot-necessity` owner decisions D1 to D5 there (the price of an adopted journal, the catch-up file's failure class, download-and-diff, whether a handover heals, new cause names) and savings S2, S3, S5, S6, S7: no decision found.

## D34. A restore that a writer keeps outpacing

- **Options:** fail with `ListingBehind` after 64 listings; take the next listing as it stands and publish its newest state.
- **Choice:** publish it, flagged as `Restored::writer_outpaced`, with a warning line on stderr and `writer_outpaced` in the command line's JSON.
- **Why:** after each of the 64 listings a read found the next file, and the listing after it showed that file, so none of them lagged; what kept the restore going is a writer. Restoring the latest state while a writer runs may answer any committed state, and this one is at least as new as every file a read found during the restore. Failing would mean no restore ever finishes against a busy writer. Lagging listings are still counted on their own, and three of them still fail.
- **Cost accepted:** the answer can be behind the writer by the time it is published, which is true of every restore of a live replica. A listing whose lag grows by exactly one file per listing for 64 listings would also end here; no store is known to do that.
- **Revisit if:** a store shows listings that lag but always show the previously read file.
- **Links:** #126, `crates/core/src/restore.rs` (`Retries`).
