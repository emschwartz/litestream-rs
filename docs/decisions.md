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
| D80 | Where can the fail-stop clock not survive a restart? | Pending staged files keep it; a boot that deletes them, unstaged WAL frames, and a process not yet booted do not | Claude (fail-stop), **delegated** | 2026-11-22 | open to owner review |
| D81 | What counts as lag for fail-stop? | Commits of the application only; engine writes and doubt snapshots taken while caught up do not | Claude (fail-stop), **delegated** | 2026-11-22 | open to owner review |
| D82 | When is the boot judged, and how does the library refuse writes? | After the first listing and the boot's snapshot; a commit hook set through the C API | Claude (fail-stop), **delegated** | 2026-11-22 | open to owner review |
| D83 | What may trip early, and what the probe needs | A forward clock step with a failing store; a trip needs a failure at or after the bound | Claude (fail-stop), **delegated** | 2026-11-22 | open to owner review |
| D84 | Fail-stop's user-visible names | As approved, with `refused_at_start`, `held_by` and `allowed` | owner, after a Fable review | 2026-11-22 | decided |
| D85 | How does the fail-stop clock date lag? | Each kind on its own date, never the start of an unbroken run; a date that cannot be kept exactly moves later | Claude (fail-stop), **delegated** | 2026-11-22 | open to owner review |
| D86 | When does a trip for age clear? | Once the commits it tripped on reach the replica, not at zero age | Claude (fail-stop), **delegated** | 2026-11-22 | open to owner review |
| D100 | How does the journal hold the unproven-WAL debt, and when is it written? | Written at the end of the tick that changes it; a new failure may raise it at any step; `JournalUnwritable` while the journal lags | Claude (journal), **delegated** | 2026-11-22 | open to owner review |
| D101 | Does a run with nothing to record still create the journal? | Yes: a process that neither read nor wrote a record writes one, so `JournalMissing` means the journal was lost | Claude (journal), **delegated** | 2026-11-22 | open to owner review |
| D102 | Are the journal's signals a degraded state? | No: a notice in `/healthz` and `lsr_journal_signal`, unwritable included; `/healthz` stays 200 | parent session, names reviewed for the owner | 2026-11-22 | decided |
| D103 | What does a boot do when it finds a recorded unproven-WAL debt and the database file may hold pages from commits the WAL lost? | Check the file against the last staged state before the TRUNCATE; a proven mix is replicated as served, noticed as `database_ahead_of_wal`, and the state before it kept 7 days, with no operator action; `STRICT` stops | Claude (journal), **delegated**, decided by a Fable review; self-clearing by the owner | 2026-11-22 | open to owner review |
| D104 | Can the engine's read mark keep other checkpoints from copying unproven WAL frames while the debt stands? | No: the mark cannot be placed at the proven frame, and the cure itself must release it; D103's check is the fix. Two marks plus a WAL rewrite is left as a spike | Claude (journal), **delegated**, decided by a Fable review | 2026-11-22 | decided |
| D130 | How does the simulator run the real directory store, and who judges it? | A filesystem trait in store-fs, an in-memory filesystem under the crash law in the sim, half the swarm on it, a staging-contract judge | Claude (sim-real-store-fs), **delegated** | 2026-11-22 | open to owner review |
| D70 | How do simulated runs draw a fault that reaches an open engine defect? | As a known-wrong cell: out of the per-push seeds, drawn by the nightly and campaigns with its failures counted as known, with a pinned seed that fails once the fix lands | parent session; mechanism by Claude (error-domain), **delegated** | 2026-11-22 | decided |
| D71 | Which errors may a simulated history inject? | Those a shipped implementation returns from that call with the world unchanged; a world fact is drawn as the world change; a cell the model cannot follow is listed as unmodelled, never drawn | Claude (error-domain), **delegated** | 2026-11-22 | open to owner review |
| D72 | How long may a simulated fault last, and what may happen inside it? | Up to 200 ticks, with a drawn sub-history; a restart inside it is drawn only where known defects are counted, since it reaches D9 | Claude (overlapping-faults), **delegated** | 2026-11-22 | open to owner review |
| D40 | An S3 replica with an empty prefix (`s3://bucket`, or `bucket` with no `path`): copy Go? | No: refuse, as before | Claude (dropin-dir), **delegated** | 2026-11-22 | open to owner review |
| D41 | `watch: true` on a `dir` entry, refused since the phase-4 plan: keep refusing under D33? | Keep refusing until built; the owner chooses | Claude (dropin-dir), **delegated** | 2026-11-22 | open to owner review |
| D42 | The config file names only one of `access-key-id` and `secret-access-key`: ignore it as Go does? | No: refuse | Claude (dropin-creds), **delegated** | 2026-11-22 | open to owner review |
| D43 | A shared AWS file holds a line that is not INI: skip it as Go does? | No: refuse | Claude (dropin-creds), **delegated** | 2026-11-22 | open to owner review |
| D44 | Credential sources that need a network call or a subprocess (instance roles, web identity, SSO, `credential_process`, assumed roles) | A gap to build under D33, tracked in #176; refused by name until then | Claude (dropin-creds), from the parent session | 2026-11-22 | gap, open |
| D45 | Config Go accepts and litestream-rs refuses at load or start (unknown and duplicate keys, values no start could use, schedules core does not run): copy Go under D33? | No: keep refusing, naming the key | Claude (dropin-config), **delegated** | 2026-11-22 | open to owner review |
| D46 | Two databases on one meta directory, replica or file, which Go starts: copy Go? | No: refuse, since Go loses data there | Claude (dropin-config), **delegated** | 2026-11-22 | open to owner review |
| D47 | Exit codes of `replicate`: one code for every error, as Go, or one per class? | One code, as Go: 1 for every failure; fail-stop keeps its own 4 | Claude (dropin-cli), from the parent session | 2026-11-22 | decided |
| D48 | Go commands, `restore` arguments, `-once` options and `/metrics` series `lsr` lacks | A gap to build under D33, tracked in #177; refused by name until then | Claude (dropin-cli), from the parent session | 2026-11-22 | gap, open |
| D50 | Boot finds the replica's newest file differing from the staged copy under the same key: snapshot or stop? Keep the stop record? | Stop unless a takeover covers it; delete the stop record | Claude, **delegated** | 2026-11-22 | open to owner review |
| D62 | What does the simulator's hold judge require of an as-of restore through a fault? | The as-of bound (G3), not the raw replica's answer | Claude, **delegated** (route from the parent session) | 2026-11-22 | open to owner review |
| D123 | Should `app_connection` keep turning `PERSIST_WAL` on, when leaving WAL mode with it on can corrupt the database? | No: it sets `wal_autocheckpoint = 0` only; the replicator's own connections keep the flag | Claude (persist-wal-vacuum), **delegated** | 2026-11-23 | open to owner review |

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
- **Cost accepted:** a false stop when the proof is gone: the meta directory deleted after a WAL restart, a VM or volume put back from an older copy, a Go run with another meta directory followed by a WAL restart. All are operator actions, and the fix is one command. The engine's own deletes of staged files the replica holds cannot cause one: before any such delete it has the driver keep a takeover record through what those files vouched for (BOOT-30), so a crash before the replacing snapshot boots clean. The deploy cycle of a host that keeps its volume (SIGTERM or SIGKILL each deploy, WORLD-41) never stops. Go never stops here, so this departs from Go; `ContinueAndInterleaveHistories` keeps Go's behaviour.
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
- **Links:** #110 (meta-directory lock, takeover record), `docs/journal.md`.

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
- **Amended 2026-11-22 (owner):** PRs land by squash-merge, so `main` keeps no merge commits; each PR becomes one commit titled by its conventional PR title. Batches of several PRs are no longer integrated as one merge commit.
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

## D50. A boot divergence is a refusal, and the stop record goes (delegated)

- **Question:** at boot the replica's newest level-0 file verifies and differs from the staged copy under the same key (BOOT-13). It used to stand while the staged files went and a snapshot followed above it. With rule (a) (D12), is that still right, and is the stop record (`lsr-stop`) still needed?
- **Choice:** refuse it with `OtherWriter` unless a takeover record covers the replica's newest key, and delete the stop record, its codec and its file. An unusable takeover record is ignored as absent and reported, never refusing the start.
- **Options considered:** keep BOOT-13's snapshot and the stop record, which alone stopped a restart after another writer took this engine's next id; refuse the start on an unusable record.
- **Why:** this engine uploads only from witnessed, synced staged copies and keeps a takeover record before deleting its own (BOOT-30), so without an operator's hand a boot divergence is another writer at this engine's next id. Refusing it covers that restart without a record, and rule (a) covers the other writer's files above staging, so the stop record kept nothing the boot does not now see. One fewer meta-directory file and crash-consistency surface. An unusable takeover record then costs only what no record costs.
- **Cost accepted:** an operator who puts back a staging backup holding another copy of the replica's newest key stops instead of snapshotting; `lsr take-over` clears it, as for D12's other operator actions.
- **Revisit if:** a world without an operator's hand produces a boot divergence.
- **Links:** `docs/BEHAVIOR.md` BOOT-13, BOOT-21, BOOT-22, CFG-38, CFG-39; `crates/core/src/actor.rs` (`boot_listing`); #110.

## D35. A stall at the end of a run

- **Options:** accept any stall as a degraded state (before); fail any stall once every injected fault has healed; fail it unless the world still imposes its fault.
- **Choice:** at the end of the final drain and after idle's settle window, a task still stalled fails the run (oracle `StuckRetrying`) unless an exhaustive table says the world still imposes that stall's fault. The table has no entry today. The run's end also heals the SQLite source failures still queued.
- **Why:** an engine stuck retrying a fault nothing causes any more, or reporting degraded for nothing, passed before.
- **Cost accepted:** a fault the world keeps imposing after the heal (none today) needs a table entry, or its runs fail.
- **Revisit if:** a fault is added that the end of the run cannot heal.
- **Links:** the stall-liveness PR; #104.

## D70. Known-wrong cells

- **Options:** let the per-push seeds go red until each defect is fixed; leave the fault out of every draw; draw it where failures are classified.
- **Choice:** a cell of the simulator's error domain that reaches an open engine defect is `Cell::KnownWrong`. The per-push fixed seeds leave it out. The nightly soak and the campaigns draw it and count a failure matching the defect's signature as known (printed, not failing the run), so every other failure in those seeds stays visible. Each defect names its tracking item and has a pinned seed in `tests/sim/known_wrong.rs` that must still fail: when the fix lands that test fails, and the cell becomes drawn.
- **Why:** the defect stays exercised every night and cannot stay known-wrong after its fix, while a push stays green.
- **Cost accepted:** a second failure with the same signature in a run that drew the cell is counted as known too; the per-push seeds do not reach the defect's code paths through these cells.
- **Revisit if:** a known-wrong cell stays open for long, or a signature turns out to match an unrelated failure.
- **Links:** issue #127 item D; `crates/sim/src/domain.rs`.

## D71. What the error domain may inject

- **Options:** inject any error into any call; inject only what an implementation returns.
- **Choice:** a cell is drawn only when lsr-sqlite (SQLite seam), store-fs (staging) or store-s3 (replica) returns that error from that call with the world unchanged. An error that reports a world fact (the WAL file replaced) is drawn as that change, and the call then reports it. A cell the simulator's model cannot follow yet is unmodelled: listed and counted, never drawn. Every other cell is impossible, with the reason in the table.
- **Why:** an injected lie (a NotFound for an object the store holds, a corrupt read of a sound object) tests a world that cannot happen, and its failures would be the simulator's, not the engine's.
- **Cost accepted:** the unmodelled cells (an unlinked WAL, a lost checkpoint retake) stay untested by the swarm until the model follows them.
- **Revisit if:** another implementation of a seam ships; its errors join the table.
- **Links:** issue #127 item D; BEHAVIOR CHK-8 and C9.

## D123. Application connections do not set `PERSIST_WAL` (delegated)

- **Defect:** `app_connection` turned `SQLITE_FCNTL_PERSIST_WAL` on for every application connection. With the flag on and `journal_size_limit` at its default (negative), `sqlite3WalClose` neither deletes nor truncates the WAL, and SQLite runs it when a connection leaves WAL mode as well as at a database's last close. A `-wal` beside a database is recovered by the next read whatever journal mode the header names, so `PRAGMA journal_mode = DELETE` answered `delete` and the database stayed on its old WAL; `PRAGMA page_size = 1024; VACUUM` then left frames of the old page size over the new file, and the same connection read "database disk image is malformed". One connection, the stock VFS, no replicator.
- **Options:** (a) document it and keep the default; (b) `app_connection` also sets `journal_size_limit` to zero or more, so the close truncates the WAL to zero bytes; (c) the engine detects a WAL whose page size differs from the database's; (d) `app_connection` stops setting the flag; (e) keep the flag and refuse `PRAGMA journal_mode` on application connections through an authorizer.
- **Choice:** (d). `app_connection` sets `wal_autocheckpoint = 0` and nothing else; `check_app_connection` checks that and nothing else; `SetupError::PersistWal` and `SetupError::PersistWalOff` are gone. The replicator's own two connections keep the flag (they never leave WAL mode). The health notice no longer advises the flag.
- **Why:** what the flag bought an application was one thing: when its connection is the database's last to close, the WAL keeps its frames and the next start continues with no snapshot. SQLite decides that in the same function, on the same two settings, as what a connection leaving WAL mode does, so no setting keeps the frames at a last close and removes them on leaving WAL mode. (b) is therefore (d) with more parts: its last close leaves an empty WAL, the next start snapshots just as it does after a deleted one, and it adds a second setting that must stay non-negative for the first to be safe (an application's own `PRAGMA journal_size_limit = -1` would arm the defect again), plus a truncation whenever an application commit restarts the log. (c) cannot fix it: the damage is done by SQLite on the application's connection with no engine running, and reading the database files from the engine's side would break the rule that nothing in the process opens them with `std::fs`. (e) takes the one authorizer slot a connection has, and an application that installs its own silently removes the guard. With (d), the connection that leaves WAL mode decides what happens to the WAL, and one without the flag deletes it, including a WAL the replicator's last close kept. The reason PLAN section 6 asked for the flag is met another way where the application follows the same section: its connections close before the replicator stops, so the replicator's close is the last and keeps the WAL.
- **Cost accepted:** an application connection that closes after the replicator's (or while no replicator runs) checkpoints and deletes the WAL, and the next start takes one snapshot of the whole database (`WalMissing`, or `GenerationChanged` when the application wrote first). Before, an in-process application set up by `app_connection` continued with no snapshot in that order too; a deploy that stops the replicator and then drops its pool now pays a snapshot per deploy until it swaps the two. A process that exits without closing its connections, or is killed, is unaffected. A public enum loses two variants. An application that turns the flag on itself is on its own; `check_app_connection` does not refuse it.
- **Revisit if:** SQLite separates the two cases (a last close and leaving WAL mode), or deploys that snapshot for want of the close order turn out to be common; the next step then is a replicator-side guard that outlives `stop`, not the flag.
- **Links:** found by the simulator running on real SQLite; sqlite `leaving_wal::a_page_size_change_outside_wal_mode_leaves_a_sound_database`, `leaving_wal::leaving_wal_mode_removes_the_wal`, `leaving_wal::a_page_size_change_after_the_replicator_closed_last_leaves_a_sound_database`, `faults::persist_wal_keeps_a_wal_a_page_size_change_makes_stale` (the SQLite belief), `the_apps_last_close_while_the_replicator_is_down_deletes_the_wal`; replica `deploy::a_deploy_whose_application_closes_last_snapshots_once_and_does_not_stop`; plant `app-connection-persists-wal`; BEHAVIOR WORLD-41; PLAN section 6.

## D121. The checkpoint-boundary snapshot trusts the database file (delegated)

- **Options:** keep checking the boundary snapshot's database pages against the page table; check only pages whose newest frame is at or below the proof; take the write lock before the TRUNCATE; stop checking at the boundary.
- **Choice:** stop checking at the boundary. The snapshot empties the page table first, as every snapshot that follows a changed WAL does, and the table then holds what the snapshot read.
- **Why:** the TRUNCATE ends the engine's read transaction, so a commit that lands after the last step is backfilled and truncated with the rest, and the boundary snapshot read its pages as damage (`DatabasePageMismatch`), a false corruption alarm under ordinary writes that stops the engine under strict responses. The per-page check needs the frames past the proof, and the completed TRUNCATE has removed them; SQLite reports a completed TRUNCATE with a log of 0. Holding the write lock across the TRUNCATE makes the TRUNCATE itself busy.
- **Cost accepted:** a page damaged between its last check and a checkpoint boundary is shipped in that snapshot, and recorded as found. Checkpoint boundaries come every 121,359 synced frames by default (CKPT-3).
- **Revisit if:** SQLite or the backend can report which frames a TRUNCATE backfilled.
- **Links:** found by the simulator's app operations inside a tick (#127 item H, seed 0xf49a2); sqlite `a_commit_between_the_proof_and_the_truncate_is_not_taken_for_damage`; plant `boundary-snapshot-keeps-page-table`.

## D120. No crash points inside the SQLite seam (delegated)

- **Options:** crash the engine between two of its calls into SQLite too (the simulator audit's item C, #127): before its committed read, WAL sync, barrier, checkpoint, sequence bump or write lock, and as a power loss inside its checkpoint with the database file torn by the block crash law; keep crash points at store calls only.
- **Choice:** store calls only. The seam crash points were built and measured, then dropped.
- **Measured:** 1000 swarm seeds per plant (seeds 1000001 to 1001000, the mixed weights), with and without them. A plant counted as found when a seed failed with it and not on the clean tree. `sim-read-lock-retake` was found (seed 6) either way; `wal-barrier` at seed 1 either way, by the direct check that a staged file's WAL frames are durable, with no crash needed. Neither tree found the issue's two plants, the sequence bumped before the TRUNCATE and the barrier released before the seal, nor `stamp-before-committed-read`.
- **Why:** the engine persists only at store calls. Every SQLite state a crash between two seam calls leaves (the barrier dropped, a checkpoint done and the bump not, a truncated WAL with no header) is one another process can also produce while the engine is down, and the simulator already draws those. A torn database file inside a checkpoint is masked by the WAL, which SQLite synced before it copied.
- **Cost accepted:** a defect that only a crash between two seam calls exposes would go unseen. None is known.
- **What it found:** a gap in the liveness oracle: a bucket rolled back past its only transaction justifies a first sync (sim `a_rollback_that_empties_the_replica_justifies_a_first_sync`).
- **Revisit if:** the engine persists anything outside a store call, or a defect turns up between two calls into SQLite.
- **Links:** #127 (item C).

## D100. The unproven-WAL debt in the engine journal

- **Question:** a failed WAL fsync must survive a restart (open defect D2). When is it written, what may follow what, and what is reported when it cannot be written?
- **Choice:** the journal's debt is a function of the engine's state, written at the end of every tick in which it differs from the last record made durable (or read at boot); boot restores a recorded debt and never forgives one. The debt is paid only through its cure (TRUNCATE owed, then snapshot owed, then none), and a new failure may raise it to TRUNCATE owed at any step: the writer refuses, as a bug, a record that forgives it. A failed write is tried again every tick; `JournalUnwritable` (approved name) is reported while the journal lags the engine and clears once nothing is left to record.
- **Options considered:** write the debt inside `Actor::fault` (it has no file lent); write it before the next sync step (one more place to remember); the design's strict order No, Truncate, Snapshot, No (refuses a second failure during the paying snapshot).
- **Cost accepted:** the record lags the failure by the rest of one tick, so a kill in that window forgets it, as without a journal. One `fdatasync` (two on the first write, which creates the file) per change of the debt; none otherwise. (The journal is now created on every run, D101.)
- **Revisit if:** the window inside the tick is shown to matter, or later facts need ordering against the debt within a tick.
- **Links:** `crates/core/src/actor/unproven.rs`, `docs/journal.md`, `docs/BEHAVIOR.md` RUN-3 and D2.

## D101. A run with nothing to record still creates the journal (delegated)

- **Question:** under D100 the journal was written only when the debt changed, so a database that never failed a WAL fsync never had one, and every restart after its first sync reported `JournalMissing`. Should a run with nothing to record write anyway?
- **Choice:** yes. At the end of a tick, a process that has neither read a record at boot nor made one durable writes the record of what it holds (no debt, today). After that, a boot reads the record and writes nothing until a fact changes. `JournalMissing` then means a journal that existed is gone (or an upgrade from a version without one), which is what an operator should be told; it is surfaced in health, `/healthz` and metrics.
- **Options considered:** keep writing only on change and stop reporting `Missing` (loses the only sign of a deleted journal); create the journal at boot, before the first tick (a boot that fails later would have written for nothing, and the end-of-tick reconcile already retries).
- **Cost accepted:** one creation per database (a write, two `fsync`s and a directory `fsync`) on the first tick of the first run, and again after a journal is lost or unreadable. A journal that cannot be created is reported as `JournalUnwritable` until a write succeeds, even while no debt is owed: the next failed WAL fsync could not be recorded either.
- **Revisit if:** creating the file on a read-only or full meta directory is shown to be common enough that the report is noise.
- **Links:** `crates/core/src/actor.rs` (`reconcile_journal`), `docs/journal.md`.

## D102. The journal's signals are notices, not degraded states

- **Question:** how does an operator see what the engine journal says, and does any of it count as degraded?
- **Choice:** `Health.journal`, the `/healthz` fields `journal` (values `missing`, `unreadable`, `newer_version`, `slot_unreadable`, `unwritable`) and `journal_detail`, and the gauge `lsr_journal_signal{db,signal}`. Every signal is a notice: `Health::is_degraded` ignores it and `/healthz` stays 200, `unwritable` included, since the engine runs as it would with no journal. `newer_version` says a newer binary wrote the journal, which points at a downgrade.
- **Options considered:** prefixed values (`journal_missing`; stutters after the field name); `unwritable` as a degraded task.
- **Cost accepted:** while the journal is unwritable, a failed WAL sync met meanwhile is forgotten by a restart (and so will any fact a later step moves into the journal, a stop among them), and nothing marks the database degraded.
- **Revisit if:** an unwritable journal proves common; then make it a degraded task that still answers 200.
- **Links:** `crates/sidecar/src/health.rs`, `crates/sidecar/src/metrics.rs`, `docs/BEHAVIOR.md` BOOT-25 to BOOT-29 and RUN-27.

## D103. A database file ahead of its WAL after a failed WAL fsync (delegated)

- **Question:** after a failed WAL fsync, a checkpoint can copy pages into the database file from the page cache, and a power loss or eviction can then lose the WAL frames they came from. SQLite recovers a shorter WAL over a newer file: a state no commit made, every page valid (simulator seeds 0x725 and 0x585). The restarted engine snapshotted it as a clean commit. What does a boot that finds a recorded debt do, and who has to act?
- **Choice:** when the WAL on disk is still the generation of the last staged file (`Recovered::Continue`), the first sync step after boot is a check, before the TRUNCATE: every database-file page that no WAL frame covers must equal its image at the newest staged transaction, which is the replica's state at its newest confirmed transaction overlaid with the pages the pending staged files wrote. No difference: the cure runs as before. A difference: the verdict (that transaction, the page count, the first page, the time) is recorded in the engine journal at the end of that tick, and the file is replicated as the application serves it. The sync task is degraded at `database_ahead_of_wal` until the cure's next step succeeds (`Responses::database_ahead_of_wal`: keep trying, or stop under `STRICT`; set through the library, not a config key; a recorded verdict is not raised again at a later boot). From the verdict, a notice stands in `Health.ahead_of_wal`, `/healthz` (`database_ahead_of_wal`) and `lsr_database_ahead_of_wal`, the degraded event is logged at error level, a boot inside the window logs how long the restore point is still kept and its end is logged too (`Event::AheadOfWal`), and retention keeps the state before the failure restorable. Seven days after the verdict the notice ends and that restore point goes back to normal retention. No operator action is needed or offered; a restart changes nothing, since the journal carries the time. A second verdict inside the window keeps the older restore point and restarts the window. When the WAL is another generation or gone, a checkpoint ran to the end and no check is possible or needed. The engine never writes the database file.
- **Options considered:** refuse to snapshot at any boot with a debt (trips on boots that are not damaged, stops backups on a disk that just failed, and a deploy restart does not come up clean); restore the damaged pages (writes under a live application that has already read and may have committed on them, and the engine has no write path to the live file); a notice at every such boot without the check (usually false); the check at every boot (a replica read of the whole database each time); degraded until an operator accepts, with `lsr accept-pages` writing an acceptance record for the sidecar (owner: it adds complication and nobody will reliably run it, so the degrade would sit for ever and teach people to ignore health); a window equal to the configured `retention` (24h by default, too short to notice a log line); holding the engine's read mark at the proven frame so no checkpoint copies an unproven frame (D104: the mark cannot be placed there and the cure must release it).
- **Cost accepted:** at such a boot, a restore of the replica's newest confirmed state into one checksum a page (about 8 bytes a page in memory) and one pass over the database file, before the TRUNCATE, which waits for it: while the replica is unreachable the cure is late, the WAL grows and the unsynced frames stay exposed to eviction. About one extra copy of the database on the replica for seven days. Retention keeps one chain, the oldest restore point asked for: while an older one is kept (a degraded task's or a failing verify's), that one is kept instead, also from before the damage. Lost commits stay lost. After the window, or for anyone who restores without reading health or the log, the latest state is the mixed file with no mark in the replica (the format has no place for one, D6). A journal lost or unwritable inside the window forgets the verdict and releases the restore point early. Not covered: a debt the journal never held (D100, D102); eviction with no restart, where the running cure can still backfill what the disk holds; a writeback failure only SQLite's own descriptor saw; a page above the last staged size that no frame covers. The degrade ends at the cure's next success rather than when the paying snapshot is staged. Left as it is: `database_page_mismatch` in a long-lived process stays degraded, with retention pinned, until a frame rewrites the page, a snapshot empties the page table or the process restarts; the sidecar has no call that accepts it.
- **Revisit if:** such boots prove common (then record the WAL's extent at the failure in the journal, so a WAL that kept every frame skips the replica read); the wait for the replica is shown to hurt; seven days proves too short or the kept copy too costly; a `database_page_mismatch` is seen standing for days (then give it the same window); a replica-side mark becomes possible once Go is no longer the fallback writer.
- **Links:** `crates/core/src/ahead.rs`, `crates/core/src/actor/ahead.rs`, `crates/core/src/actor/unproven.rs`, `crates/core/src/pages.rs`, `docs/journal.md`, `docs/BEHAVIOR.md` RUN-3, BOOT-31 and D2, D100, D102, D104, simulator seeds 0x725 and 0x585.

## D104. Holding the read mark at the proven frame while a WAL fsync debt stands (delegated)

- **Question:** the replay of simulator seed 0x725 shows the mix of D103 made while the engine was alive: the application's PASSIVE checkpoint copied unproven frames under our read mark, its TRUNCATE and our own cure copied more and went busy behind another reader, and the WAL was never reset before the power loss. Should the engine keep its read mark at the last frame proven durable while the debt stands, so no checkpoint can copy an unproven frame?
- **Choice:** no. SQLite never backfills past the lowest active read mark, so the idea is sound, but the engine cannot do it. A read transaction starts at the current end of the WAL, so the mark it holds when a sync fails may already cover the unproven frames (it did in 0x725), and it cannot be lowered. The cure must end that read transaction and backfill those frames; when another reader blocks the reset, SQLite has already copied them before it reports busy, so the exposed state is left by the cure attempt itself. D103's check at the next boot stays the fix.
- **Options considered:** not retaking the read transaction while the debt stands (the mark is already past the proven frame, and every cure attempt releases it); a second reader connection whose mark only moves after the fsync that covers it (closes the time before the first cure attempt and nothing after); that second mark together with a cure that writes the unproven frames' bytes back into the WAL and syncs them, with no TRUNCATE (would close it while the engine lives, but the engine would write SQLite's WAL); waiting to run the cure until no reader blocks it (readers in our own process cannot be seen).
- **Cost accepted:** between a failed WAL fsync and a completed TRUNCATE, any checkpoint, ours included, can put unproven pages in the database file, for as long as another reader blocks the reset. A power loss or eviction then leaves the mix, which is found only at the next boot (D103), after the application has served it.
- **Revisit if:** the mix is seen outside the simulator, or the two-mark and rewrite design is shown safe on the real-SQLite rig under the fault VFS (no reset under the held mark, no write past the committed end, the eviction case caught by the verified read).
- **Links:** D103, `crates/sqlite/src/checkpoint.rs`, `crates/core/src/actor/unproven.rs`, `docs/BEHAVIOR.md` RUN-3 and RUN-22, simulator seed 0x725.

## D130. The real directory store in the simulator (delegated)

- **Options:** keep `MemStore` with its power-loss bookkeeping as the only staging double; run `FsStore` on a temporary directory of the host (no crash law, no determinism); a filesystem trait in `lsr-store-fs` with an in-memory filesystem in `lsr-sim`.
- **Choice:** the trait (`Fs`, `FsFile`, `RealFs`; `FsStore<F: Fs = RealFs>`, so every existing user is unchanged) and `MemFs`. A file keeps its synced and current images under the block law the WAL uses; a directory entry changed since its directory's last sync follows the one directory-entry rule of `crash_law.rs` (`entry_kept`): under the block law each such entry keeps its change or goes back to what it durably held, on its own coin; under all-or-nothing a deletion stays done and any other change is undone. Half of the swarm mixes (drawn last, off in the default mix) put staging on `FsStore<MemFs>`. A judge in the runner fails a run when a staged file whose commit or delete returned success changed across a power loss (`Violation::StagedReverted`).
- **Why:** `MemStore`'s power loss assumes the store syncs what it should; the three durability plants in `store-fs` could only be killed by their named crash-replay tests. With the store itself under the crash law the swarm finds all three unaided.
- **Cost accepted:** the judge checks the store's contract, not the engine's behaviour, so it finds a store defect the engine would survive (the delete plant). Per-entry independence can split a rename (both names, or neither), which ext4 would not; it is the rule the WAL's fault VFS and `Volatile` already use, so the disk stays one law (#121). A fault the fault layer injects after publishing (`ErrorAfterPublish`) does not unsync anything on `MemFs`: the store did sync, so in this profile such a file always survives.
- **Revisit if:** the shared directory-entry rule changes; or `MemFs` and the host disagree in `differential::the_doubles_answer_as_the_real_store_does`.
- **Links:** #127 item I.

## D40. An S3 replica with an empty prefix (delegated)

- **Question:** Go accepts an S3 replica with no prefix and writes keys with a leading slash (`/0000/...`, `rc.go` at 041a8c3). litestream-rs refuses it at load (ruling F10, `docs/plans/phase-1-reading.md`). D33 asks for Go's behaviour wherever Go is not wrong.
- **Options:** copy Go (keys with a leading slash, in the S3 store); keep refusing.
- **Choice:** keep refusing, for now.
- **Cost accepted:** a Go config with a bare bucket does not start under litestream-rs, and a replica Go wrote that way cannot be read without a store change. The refusal names the key, so nothing replicates less than asked.
- **Why:** copying Go is a change to the S3 store's key layout (an engine crate), not to the config, and a leading-slash key is a layout few S3 tools handle well. The `dir` differential (`compat::go_and_rust_scan_the_same_databases`) found it with a `dir` naming a database file, where the relative path is `.` and adds nothing to an empty `path`.
- **Revisit if:** the owner wants exact parity here; then the store learns the leading-slash layout and a compat test writes and reads it both ways.
- **Links:** `docs/BEHAVIOR.md` CFG-18, `crates/store-s3/src/config.rs` (`ConfigError::EmptyPrefix`).

## D41. `watch: true` on a `dir` entry (delegated)

- **Question:** Go's `watch` adds and removes databases at run time as files appear under a `dir` (`directory_watcher.go`). litestream-rs refuses the key at load (the phase-4 plan's ruling O5, option B). D33 makes litestream-rs a full drop-in, which reopens it.
- **Options:** build it (start and stop replicators at run time, a rescan that never opens a file a replicator holds, an inotify equivalent); keep refusing.
- **Choice:** keep refusing until the owner chooses to build it. The `dir` work on `claude/dropin-dir` brought the static scan to Go's behaviour and left `watch` alone.
- **Cost accepted:** a Go config with `watch: true` does not start; a database added under a `dir` is replicated only after a restart (CFG-17).
- **Why:** it is a feature of its own, with run-time actor lifecycles the simulator does not drive, not a correction. A refusal replicates nothing silently less than asked.
- **Revisit if:** the owner wants it; it then needs its own plan and behaviour rows (a file created, renamed, removed and recreated under a watched directory, each against Go).
- **Links:** `docs/BEHAVIOR.md` CFG-15, CFG-17; `docs/plans/phase-4-sidecar.md` O5.

## D72. Overlapping faults

- **Options:** keep holds short and empty (at most 11 ticks of commits and ticks); let them last and run anything inside; let them last and run anything but what reaches an open defect.
- **Choice:** under the overlap draw a hold lasts up to 200 ticks and runs a drawn sub-history (commits, ticks, one-shot store faults). A process crash, a power loss or a shutdown inside a hold is drawn only where known defects are counted (D70), because each reaches D9 (a halt across restarts goes unreported). A lying disk keeps its short length, since its own power loss is such a restart.
- **Why:** the defects that live where faults overlap (D9, BOOT-21) were unreachable; reaching them without turning the per-push seeds red needs the known-wrong rule.
- **Cost accepted:** the per-push seeds draw no restart inside a hold until D9 is fixed; a run is longer, so the read budget gains a term for a degraded task's retries and the retry-wait check gains the slew's slack after a clock step back.
- **Revisit if:** D9 is fixed (then restarts inside holds are drawn everywhere), or holds longer than 200 ticks find more.
- **Links:** issue #127 item E; BEHAVIOR CHK-9.

## D73. A database file no commit made, after a lost writeback (delegated, open to owner review)

- **Options:** adopt the recovered database as the new truth (any restore of it passes); adopt it as damage, as a flipped WAL bit is (no restore may give that exact state); keep the world's model as SQLite's rules say it should be, so the model check fails the run where the mix appears.
- **Choice:** the model check, counted as the known-wrong defect `DatabaseAheadOfWal` (D70) on the failed-sync cell, pinned to seed 0x585. Adopting it as damage was tried first and dropped.
- **Why:** the state is real. A failed fsync leaves the kernel's pages clean but unwritten; a later checkpoint by another connection backfills the real pages into the database file and syncs it; a power loss then cuts the WAL at the first frame the kernel lost, and the file holds newer pages from the backfill and older ones from the recovered WAL. Adopting it as truth would let a silently wrong replica pass. Adopting it as damage refuses only a restore of that exact state: later commits build on the mixed pages, the model follows them, and the engine's replication of them passes. With the engine journal merged no seed of 4000 failed that way, so the defect was hidden. The model check fails every run that reaches the mix.
- **Cost accepted:** every such run fails, whatever the engine does next, so it stays known-wrong until the engine detects the mix (the journal work's D103: a boot after a recorded failed sync compares the pages the WAL does not cover and replicates a mixed file loudly, `database_ahead_of_wal`). The cell flips then, with the world adopting the state and a judge allowance that requires the notice before the file is staged.
- **Revisit if:** D103 lands, or the owner prefers adopting the mixed file as truth.
- **Links:** issue #127 item E; D2, D103.

## D80. Where the fail-stop clock cannot survive a restart (delegated)

- **Question:** the clock of the oldest unreplicated commit must not restart with the process (#115), or a crash loop runs the application forever in slices shorter than the bound. What does it rest on?
- **Choice:** the stamp of the oldest pending staged file (the witness token keeps it); unstaged WAL frames date from this process's first sighting of them (D85). Three cases are not covered: (1) a boot that cannot vouch for its pending staged files deletes them and stages a snapshot stamped at that boot, so the clock moves once, there; (2) WAL frames no life managed to stage carry no time and count from the life that sees them; (3) before the first boot of a process nothing says which staged files the replica lacks, so the clock covers nothing and only a stop or a refusal can trip.
- **Options considered:** a floor taken from the deleted files' created-at times, built and then removed: after a bucket rollback it dated a deleted file that changed nothing as old content, a false trip the simulator's judge found, and a new process cannot read a deleted file. WAL mtime (understates) and the replica's newest file time (overstates and trips falsely).
- **Cost accepted:** a crash loop whose every boot deletes its pending files, or whose every life fails to stage, never trips for age.
- **Revisit if:** the journal can carry a mark ("unreplicated since") that the engine writes when lag starts and clears at zero; that closes all three.
- **Links:** #115, #140 (the journal mark), `crates/core/src/failstop.rs`, `crates/core/src/actor/lag.rs`, `sim::a_crash_loop_trips_for_age`.

## D81. What counts as lag for fail-stop (delegated)

- **Question:** the false-trip judge, run over random simulator histories, found the clock running with nothing of the application's off the replica. Which staged files are lag?
- **Choice:** every staged file the replica lacks, except: a file that changes no page against the page checksum table at the same size (the driver's start and the frame after each checkpoint rewrite page 1 unchanged; a boot finds such files among the pending ones while it seeds the table); and a snapshot staged for a doubt alone (an operator's request, a WAL whose continuity cannot be proven) while nothing was off the replica. A snapshot owed for a doubt alone no longer keeps earlier lag running; one owed because commits went past staging (a TRUNCATE after a failed WAL sync, a checkpoint past the synced end) does.
- **Cost accepted:** a commit made and checkpointed away by another process between two ticks while nothing was off the replica is missed until the next commit starts the clock: a late trip, never a false one.
- **Amended (false trips found by the simulator on main):** the rule that a file changing no page is not lag also covers the files a checkpoint stages (the seal, the frame that moves the WAL past a restart), which had skipped it. And while a snapshot is owed for a doubt alone with no synced position and every staged file on the replica, the WAL end when the doubt is first observed is a baseline: frames up to it, and a snapshot that ends at it, are not lag. A commit that lands between the boot and that observation is missed until the snapshot stages it, again late, never false.
- **Revisit if:** the doubt causes change, or a fault appears that puts commits only in a doubt snapshot.
- **Links:** #115, `actor/lag.rs` (`doubt_only`, `note_quiet_file`, `note_doubt_baseline`), `sync.rs` (`changes_pages`), `pages.rs` (`Seeded`), `checkpoint.rs` (`Staged::quiet`).

## D82. When the boot is judged, and how the library refuses writes (delegated)

- **Question:** under `hold-start` the application is held only before it is released. When may it be released, and how does the library refuse a write?
- **Choice:** the boot is judged once the replica was listed and the snapshot the boot asked for, if any, was staged or its step failed (a stop that snapshot finds is a stop at boot); or once a boot attempt failed for anything but a refusal (a store that times out does not hold the start). Until then the gate is `Pending`: an `-exec` child waits, library writes are not refused, nothing is released. The library refuses through a commit hook `app_connection` installs with the C API in `lsr-sqlite` (rusqlite's own needs a mutable connection, and `app_connection` takes a shared one); the hook's flag is one `Arc` count per connection, never freed. An application on another binding reads the gate from health.
- **Cost accepted:** the hook replaces any commit hook the application set. A library application that writes before the first boot is judged is not refused even if the boot then stops; `wait_booted` is there for one that must not.
- **Revisit if:** applications need their own commit hook, or ask for writes refused while pending.
- **Links:** #115, `crates/sqlite/src/gate.rs`, `crates/replica/src/thread.rs` (`Worker::fail_stop`).

## D83. What may trip early, and what the probe needs (delegated)

- **Question:** the design trips when the age passes the bound and the probe at the bound fails. Which failures count, and on what clock?
- **Choice:** only a failure of the sync or upload task at or after the bound's instant trips; an earlier failure followed by a quiet backoff does not. Age is measured on the wall clock the engine reads, so a step forward is time passing: with a failing store it may trip early. With a healthy store the probe succeeds and nothing trips. The simulator's judge measures age on both clocks and takes the larger.
- **Cost accepted:** a forward clock step during a store outage can trip `stop-app` before the bound has truly passed.
- **Revisit if:** the environment gives the engine a monotonic clock that survives restarts.
- **Links:** #115, `crates/sim/src/failstop.rs`.

## D84. Fail-stop's user-visible names

- **Choice (owner, after a Fable review of the proposals in `docs/proposals/fail-stop.md` section 4):**
  - config: `if-replication-stops` (`report`, `hold-start`, `stop-app`) and `max-unreplicated-age` (10m, at least 5m), under `litestream-rs:`;
  - cause names: `unreplicated_too_long` and `refused_at_start` (proposed `not_yet_reachable`);
  - `/healthz`: the field `held_by` (proposed `fail_stop`) names the cause, and `app` is `pending`, `allowed` or `held` (proposed `open` for `allowed`);
  - log lines: "start refused" and "app stopped", each with the cause name as `why`;
  - `-exec` exit code 4, help line "replicate -exec: replication stopped, so the application was stopped or never started (if-replication-stops)";
  - config-check refusal: "if-replication-stops: stop-app refused: stopping the application needs -exec (or the exec key); use hold-start or report";
  - library: `Health::held_by`, `Gate::{Pending, Allowed, Held}`, `Trip::{Stopped, UnreplicatedTooLong, RefusedAtStart}`.
- **Dropped before review:** `Replicator::writes_refused` and the `lsr_app_held` metric; health and `/healthz` carry the same state.
- **Revisit if:** operators misread any of them in practice.
- **Links:** #115, #149, `crates/sidecar/src/failstop.rs`, `crates/cli/src/run.rs` (the exit table).

## D85. How the fail-stop clock dates lag (delegated)

- **Question:** the clock took the first time this process saw lag, kept while lag never fell to zero. A writer whose uploads trail by a tick always has lag, so the clock grew without bound though each commit landed in seconds, and one failed upload after the bound tripped (seeds 0x330, 0x9f9, 0x56a). What date does lag carry?
- **Choice:** each kind of lag carries its own date, and the clock is the oldest: a pending file with a commit, its stamp; unstaged WAL frames, the observation that first saw the WAL end where it did; frames once staged, that date, carried to the first file staged after they were seen and dropped when the replica holds that file; lag with no date of its own, its first sighting while it lasts.
- **Options considered:** a date per staged file, kept in a map bounded by the pending files: exact, but more state for a gain of at most a tick. Carrying the date to the newest pending file: found by the simulator to date a newer commit's file with an older run's time.
- **Cost accepted:** every rule errs late. A run of frames that spans several files loses its date to their stamps after the first one lands; a second run staged while the first still carries its date loses its own; both can trip later than the bound by up to the time between seeing frames and staging them.
- **Revisit if:** a trip is found late by more than a tick, or the journal mark (#140) gives frames a durable date.
- **Links:** `crates/core/src/failstop.rs` (`Lag`, `WalMark`, `Observed`), `actor/lag.rs`, core::`a_steady_writer_whose_uploads_trail_by_a_tick_never_trips`.

## D86. When a trip for age clears (delegated)

- **Question:** a trip for age cleared only at zero age, so it would not flap at the bound. A busy database may never reach zero: seed 0xa4d held a trip whose commits had all landed because one later frame stayed unstaged.
- **Choice:** the trip clears once the clock's start has moved past the start it tripped on: every commit that old is on the replica. A new trip needs a new bound and a failure at or after it, so writes still do not flap at the edge.
- **Cost accepted:** after a long outage heals, a new trip can follow as soon as newer lag reaches the bound and fails again, without the age touching zero in between.
- **Revisit if:** operators see a trip clear and come back within one bound.
- **Links:** `crates/core/src/failstop.rs` (`Lag::observe`), core::`failstop::tests::a_trip_for_age_clears_once_its_commits_land`.
## D42. A lone `access-key-id` or `secret-access-key` (delegated)

- **Question:** Go builds static credentials only when the config file gives both keys; with one, it ignores it and signs with whatever the AWS SDK's default chain finds (environment, shared files, the instance role). litestream-rs refuses the file. D33 asks for Go's behaviour unless Go is wrong.
- **Options:** copy Go; refuse.
- **Choice:** refuse, as before.
- **Cost accepted:** a Go config with one key does not start until it is fixed.
- **Why:** AGENTS.md's contract: a request that cannot be met exactly is an error, never something else. Go signs with credentials the file did not name, possibly another account's, and says nothing. No data is lost either way, so this is a call on the contract, not on safety.
- **Revisit if:** the owner prefers exact parity here.
- **Links:** `docs/BEHAVIOR.md` CFG-21; `crates/config/src/map/replica.rs` (`HalfCredentials`); `compat::go_and_rust_resolve_the_same_credentials` asserts the refusal.

## D43. A line in a shared AWS file that is not INI (delegated)

- **Question:** the AWS SDK's INI parser skips a line that is not a header, a property or a continuation; litestream-rs refuses the file.
- **Options:** skip, as Go; refuse.
- **Choice:** refuse, as before.
- **Cost accepted:** a file Go reads does not start litestream-rs.
- **Why:** the usual such line is a header missing its `]`. Skipping it files the keys below it under the profile above, so Go signs with the wrong profile's keys and says nothing. The AWS CLI's parser refuses such a file too.
- **Revisit if:** a real file with a harmless non-INI line turns up.
- **Links:** `docs/BEHAVIOR.md` CFG-22; `crates/store-s3/src/chain/ini.rs`.

## D44. Credential sources that need a network call or a subprocess: a gap to build

- **Question:** Go's SDK resolves EC2 instance roles, ECS and EKS container credentials, web-identity tokens, assumed roles (`role_arn` with `source_profile` or `credential_source`), SSO and `credential_process`. litestream-rs builds none of them (CFG-7), so an application on EC2, ECS or EKS with a role and no static keys does not start under `lsr`.
- **Choice:** not an accepted trade-off. D33 (full drop-in) requires these, and they are tracked as a gap to build in #176, which names the Go code to match and the differential that will prove it. Until then the chain finds no keys and the start fails naming the sources it tried.
- **Cost until built:** role-based deployments need static keys under litestream-rs.
- **Evidence of the gap:** `compat::go_resolves_what_rust_does_not_build` asserts it, so building any source shows there first.
- **Links:** #176; `docs/BEHAVIOR.md` CFG-7; `crates/store-s3/src/chain.rs`.

## D45. Config refusals stricter than Go (delegated)

- **Question:** the config differential (`compat::go_and_rust_parse_the_same_config`) lists every file Go starts and litestream-rs refuses. D33 asks for Go's behaviour unless Go is wrong. Which refusals stay?
- **Options:** copy Go for each; keep each refusal.
- **Choice:** keep these, each named in the test so a new one fails it:
  - an unknown key (CFG-1) and a key set twice (CFG-28): Go ignores the first and keeps the last value of the second, so a typo in `retention` or a pasted second block silently changes what is replicated;
  - values no start could use (CFG-26): a signing region with whitespace, `/` or a control character, a `part-size` under 5 MiB, `monitor-interval` zero or less, a duration under a millisecond or negative where the engine counts milliseconds. Go starts and then fails every request, every upload or every poll, or (for a bare `10`, read as 10 ns) spins;
  - schedules core does not run (CFG-27): levels not increasing, a snapshot interval not above the top level, a retention below the interval, and TRUNCATE without the `app-readers: none` claim (`docs/plans/phase-4-sidecar.md`).
- **Cost accepted:** a file Go starts may not start litestream-rs until it is fixed. Each refusal names the key and line.
- **Why:** AGENTS.md's contract: never silently wrong. None of Go's behaviours here replicates what the file says; each either ignores part of it or fails later and quietly.
- **Also accepted:** on R2 and Tigris, an explicit `concurrency: 0` or `part-size: 0` gives Go the SDK's generic default rather than the provider's; litestream-rs uses the provider's. Upload parallelism only.
- **Revisit if:** the owner wants a file that starts Go to start litestream-rs whatever it holds.
- **Links:** `docs/BEHAVIOR.md` CFG-1, CFG-26 to CFG-28; `crates/compat/tests/compat/dropin_config.rs` (`DEPARTURES`).

## D46. Two databases on one meta directory, replica or file (delegated)

- **Question:** Go refuses a shared meta directory only between databases of one `dir` entry, and a repeated `path` only when spelled identically. Two `path` entries with one `meta-path`, two databases on one replica prefix, or a database named by a `path` and found by a `dir` all start under Go.
- **Options:** copy Go; refuse.
- **Choice:** refuse, as before.
- **Cost accepted:** none known: no working setup has two replicators sharing staging or a prefix.
- **Why:** this is a case where Go's behaviour loses data. Two replicators on one meta directory overwrite each other's staged files (Go's own comment on its `dir` check calls it "replication state corruption"); two on one prefix interleave two histories, and retention then deletes one's files.
- **Revisit if:** never, unless the layout changes.
- **Links:** `docs/BEHAVIOR.md` CFG-29; `crates/config/src/map.rs` (`distinct`).

## D47. Exit codes of `replicate`: as Go

- **Question:** Go Litestream exits 1 on every error and 0 for literal help (`cmd/litestream/main.go:89-104`). `lsr replicate` exited 1, 2, 3 or 64 by class.
- **Choice:** `replicate` exits 1 on every failure, as Go, because a script that checks for Go's 1 would otherwise break. One exception stays: 4, when fail-stop holds or stops the application (D19, approved by the owner in D84), a state Go never reaches. `lsr`'s own commands (`verify`, `files`, `config check`) keep a code per class, since no Go script runs them.
- **Cost accepted:** a supervisor can no longer tell a refused config from a missing one by the code; the error line says which.
- **Not covered:** `lsr restore` does not take Go's `restore` arguments (`-config`, `-o`, a database path), so a Go script fails it before its code matters; tracked in #177. #41 stays open for the codes of `lsr`'s own commands.
- **Links:** `docs/BEHAVIOR.md` CFG-34; `crates/cli/src/main.rs`.

## D48. Command-line and endpoint surface Go has and `lsr` lacks: a gap to build

- **Question:** comparing Go 041a8c3's commands, flags and endpoints with `lsr` found missing: the commands `databases`, `reset`, `status`, `sync`, `list`, `info`, `ltx`, `wal` and the Windows service commands; Go's `restore` arguments (`lsr restore` has its own); `replicate -once -force-snapshot` and `-once -enforce-retention`; eleven Go `/metrics` series and pprof (CFG-35).
- **Choice:** not an accepted trade-off. D33 (full drop-in) requires them, and they are tracked as a gap to build in #177, which names the Go code to match and the differential that will prove each. Until then each is refused by name.
- **Cost until built:** a deployment that runs one of these commands, or graphs one of these series, needs Go Litestream for it.
- **Evidence of the gap:** `compat::go_and_lsr_read_the_same_replicate_command_lines` accepts the `-once` refusals only by their message, tied here.
- **Links:** #177; `docs/BEHAVIOR.md` CFG-33, CFG-35.

## D74. Every engine commit checks the size the store acknowledged (delegated)

- **Options:** keep the check in the uploader alone (which retried but left the short object listed) and in `Retrying`, which the engine never wraps its stores in; check in each engine call site; one upload type every engine commit goes through.
- **Choice:** one type, `Checked` (`crates/core/src/checked.rs`; since D75 `publish::Writer` in `crates/core/src/publish.rs`). The uploader, the compactor and the staging writer open uploads only through it. It counts the bytes written; a commit acknowledging another size deletes the object, then reports `CommitSizeMismatch { removed }`. `Retrying` settles its own commits with the same function, so the rule is written once.
- **Why:** the compactor took a short acknowledgement as a published output and maintenance then deleted its inputs, so `restore Latest` failed to decode the replica (simulator seed 0x46, once the simulator's size-mismatch fault published a short object). The uploader saw the mismatch but left the object listed until its retry, so restores failed in between.
- **Cost accepted:** a delete after every mismatched commit. A store that misreports sizes on objects it holds whole loses them to the delete and they are written again; the caller still holds the bytes until a verified listing. The type does not stop a future engine function from calling `begin_upload` directly; the planted bugs `compact-commit-unchecked` and `short-commit-object-kept` catch the two paths that exist.
- **Revisit if:** a new engine path writes to a store.
- **Links:** #127 (the blind-plant pilot's second gap), `docs/planted-bugs.toml`.

## D111. What the soak must reach (delegated)

- **Options:** keep reach keyed by state name and gated on 64 fixed seeds; key it by behaviour row and fault class and gate every night's soak on zeros.
- **Choice:** both. `mark!(<Row>)` on the detecting branch in lsr-core, compiled only under `verify`; each `docs/behavior.toml` row says `mark = "site"` or `"none: <reason>"`. Every injector variant maps through a wildcard-free match to a fault class counted drawn, applied, skipped, hit, judged during and judged after. The nightly soak fails when a sited row or a required class count is zero; `canary.never` must be the only zero. A class the mixes never draw is `Undrawn` with its reason and must stay undrawn. A required count that cannot be earned is listed in `EXEMPT` (`crates/sim/src/marks.rs`) with its reason. "A weight set to 0 goes red" is an in-process test on every push (the second writer's weight), not a nightly scratch tree.
- **Why:** a green soak said nothing about what it exercised (issue #127, section 4). A count per row turns "covered" from a reading of the generator into a number from a run.
- **Cost accepted:** `hit` is exact only for store faults and injected SQLite failures; for every other class it means an engine tick ran with the fault in place or after it, which is not proof the damage was read. A row whose detection sits in a `const fn` mapping arm has no site yet ("not yet placed"). A rare class or row that a short night misses makes that night red until a floor or an exemption says otherwise.
- **Revisit if:** a night goes red on a key that a longer soak reaches (add a floor per key), or the marks start deriving BEHAVIOR's D, S and R (section 7.3, item 2).
- **Links:** #127.

## D62. The hold judge holds an as-of answer to the bound

- **The case:** a lagging listing hides a merged level-1 file. An as-of restore through the hold's view plans from the finer level-0 files under it; the raw replica plans from the merged file and, for an instant inside it, takes the earlier state. The two answers differ, and the hold judge failed the run for not matching the raw one (simulator seed 0x80e).
- **Options:** (a) the judge demands equality with the raw replica for every target, as before; (b) the as-of planner prefers the finest files so both answers agree; (c) the judge holds an as-of answer that differs from the raw one to the as-of bound.
- **Choice:** (c). G3 in `docs/BEHAVIOR.md` promises only that an as-of answer holds no commit begun after the instant, and says any older state meets it. Both answers do, so the judge was wrong, not the planner. `Latest` and a transaction id keep the equality check: their answer is fixed by the replica.
- **Cost accepted:** a view that answered as of an instant with a state too old for its own files is not caught by this judge; the history judge's lag checks are where staleness is bounded. Under a backward clock step the history judge checks only that the answer is a state of the history (its untimed path), so the pinned 0x80e history exercises that path.
- **Revisit if:** the as-of guarantee gains a lower bound (an answer at least as new as the newest file before the instant).
- **Links:** `crates/sim/src/run/hold.rs` (`judge_through`), `crates/sim/src/history.rs` (`judge_as_of`), G3 in `docs/BEHAVIOR.md`.

## D75. One module writes to and deletes from the stores (delegated)

- **Options:** keep each rule (the size check, delete only what a rejected write created, why a delete is allowed) at the call sites that need it; one module that owns every engine write and delete, enforced by review; the same module enforced by a check that fails the build's gate.
- **Choice:** `crate::publish` owns them, enforced by a source guard (`publish::guard`, a test `just check` runs). Its `Writer` is the only way to write: it does not implement `Upload`, so its commit can only return a `Published` (the commit checked against the bytes written, saying whether the write created or replaced the object) or an `Unpublished`. Only a rejected create-only write hands back a `Rejected`, and `remove_rejected` deletes nothing else. Every other replica delete names a reason (`Removal`: retention, a rotten object a later file covers, a rejected snapshot or compaction output); staged files go through `remove_staged`. The guard counts calls of a store's `begin_upload` and `delete` in lsr-core's non-test source and lets through only the store implementations and the wrappers that pass calls through, each listed with why.
- **Why:** three engine bugs were one rule missing on one path (#145, 0x191, the unchecked compaction commit): issue #171 item 1. A rule in one module, with no other path to the store, cannot be missed on a new path.
- **Cost accepted:** the guard is a scan of the source, not the type system: it reads call names, so a store call written another way (a function pointer, a macro) would pass it, and it covers lsr-core only. Staging and replica still share one store trait and one error type (the next step makes them distinct). A key a read shows already holds our bytes is not yet a `Published`; item 4 of #171 adds it with the proof types that consume it.
- **Revisit if:** another crate writes to a store for the engine, or the store trait can be split so that raw writes are unreachable by type.
- **Links:** issue #171 item 1; D74; `crates/core/src/publish.rs`.

## D78. A replace acknowledged short is never deleted (delegated)

- **Question:** D74 deletes every object a store acknowledges with another size than was written, so the engine never leaves a listable object it knows is short. Simulator seed 0x1dc shows the cost: a boot re-uploads a staged file the replica already holds, a read that flips a byte makes the good copy look torn, the uploader replaces it, the store acknowledges the replace one byte short, and the delete leaves the replica ending one transaction early. Restores of the latest state then silently lose an acknowledged transaction. Which writes may delete after a size mismatch?
- **Choice:** only a create-only write (`WriteMode::New`): the bucket refused a taken key before it took any bytes, so the object is the write's own. A replace deletes nothing and reports `removed: false` (`publish::settle`, and `Retrying`, which the command line wraps S3 in, through the same function). The short object makes a restore that meets it stop with a typed error until the caller writes the key again (the uploader retries, `UploadAmbiguous`) or removes it under a reason of its own (maintenance's executor deletes a short compaction output, whose inputs still cover the range). The simulator's store counts a short object a replace left as damage at once, as it counts one whose delete failed.
- **Options considered:** keep D74's delete for every write (silent loss of an acknowledged transaction, against the contract's "an error, never a shorter history"); read the key before every replace to learn whether it held something (a read per write, and a race with any other writer); re-put inside `publish` (it holds no bytes; the caller does, and retries already).
- **Cost accepted:** after a short replace, until the retry, the key holds a short object: a restore that needs it fails loudly, and a listing shows a key that does not decode, which the caller must not trust (`removed: false` says so). The replace had already overwritten the good bytes, so nothing restorable is lost by keeping it.
- **Revisit if:** a store is found that keeps the old object when a replace is acknowledged short (then the delete would have destroyed a good object and the keep is right twice over), or the engine gains a replace that knows the key was free.
- **Links:** D74, D75; `crates/core/src/publish.rs`, `crates/core/src/retry.rs`, `crates/sim/src/store.rs`; simulator seed 0x1dc (the journal session's `KnownWrong::AckForgottenAtRestart`).

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
