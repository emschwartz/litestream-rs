# `if-replication-stops`: what trips it (#115)

Read-only design from `crates/core/src/fault.rs` on origin/main. Nothing built or run.

## Decision

Three triggers, and only three (trigger 3 added 2026-11-22, delegated decision, see docs/decisions.md D16):

1. **Unsafe stop.** The engine is stopped (`StopCause` other than `Shutdown`). Trips at once. No bound: a stop is terminal, waiting heals nothing.
2. **Unreplicated too long.** A commit has been off the replica longer than `max-unreplicated-age`, and a probe made at the bound also failed. The cause of the delay does not matter; it is only the label.

3. **Refused at boot.** Before this process has listed the replica once, the store refuses it (`StoreDenied`: `credentials`, `permission_denied`, `clock_skew`, `bucket_missing`, `tls`, as classified in #108). The app is held until the first listing succeeds, then released with no operator action. Timeouts, 5xx and throttling never hold. After the first successful listing a denial is an ordinary degraded state and falls under trigger 2.

Nothing else trips: not a fault count, not a degraded flag, not `Stalled`. A task that is degraded while uploads still land (age stays 0) never trips. That one rule is what keeps blips, throttling and maintenance faults from taking the app down.

| Value | Unsafe stop at boot | Unsafe stop while running | Unreplicated too long |
|---|---|---|---|
| `report` | 503 | 503 | 503 |
| `hold-start` (default) | app not started / writes refused | 503 only | 503 only |
| `stop-app` | as hold-start | app stopped | app stopped; at boot, held |

Trigger 3 (refused at boot) holds under both `hold-start` and `stop-app`, and is 503 under `report`.

**Against PLAN.md D7.** The engine still keeps trying on every cause, as Go does; none of this changes a retry. What changes: the one default stop (another writer) now also applies at boot (rule (a)), and under `-exec` or the library the app's start waits on an unsafe stop or a boot refusal, where Go starts the child regardless. Plain sidecar behaves as Go plus 503. A running app is never stopped by default.

## 1. Conditions

Bound "age" = `max-unreplicated-age`. "Never" = cannot trip by itself; it trips only if it keeps a commit off the replica past the bound.

### Stops (engine stopped)

With the default `Responses::GO_LIKE` only `OtherWriter` stops. The others stop only when the operator set that cause to `Stop` (or `STRICT`).

| StopCause | hold-start (boot) | stop-app (running) | Bound |
|---|---|---|---|
| `OtherWriter` (incl. the new boot rule: unvouched replica files, and a kept stop record) | yes | yes | none |
| `Credentials`, `StoreRefused` | yes | yes | none |
| `RemoteCorrupt`, `RemoteRegressed` | yes | yes | none |
| `WalCorrupt`, `WalTruncated`, `WalUnlinked`, `WalSyncFailed` | yes | yes | none |
| `PageSizeChanged`, `DatabasePageMismatch` | yes | yes | none |
| `Bug` | yes | yes | none |
| `Shutdown` | no | no | n/a (a drain; the app is going down anyway) |

### Persistent causes kept trying (degraded, default response)

| PersistentCause (KeepTrying) | Uploads continue? | Trips |
|---|---|---|
| `Credentials`, `StoreRefused` | no | at age |
| `RemoteCorrupt` | uploads may; maintenance does not | at age, only if uploads are blocked |
| `RemoteRegressed` (relist) | yes after the relist | never, unless the relist keeps failing |
| `WalCorrupt`, `WalTruncated`, `WalUnlinked` | no, until a new generation | at age |
| `WalSyncFailed` (TRUNCATE + snapshot) | yes | never, unless the snapshot cannot land |
| `PageSizeChanged` | no (never writes the new size) | at age |
| `DatabasePageMismatch` | yes (ships the page) | never |
| `Bug` (rebuild) | yes after the rebuild | never, unless it loops |
| `Stalled(_)` | see below | at age |

### Retried faults (`Stall` variants; degraded after 8 failures, about 127 s)

| Fault | What it is | Trips |
|---|---|---|
| `StoreTransient`, `StoreThrottled`, `UploadAmbiguous`, `UploadChecksumMismatch`, `RemoteObjectMissing` | remote unreachable, slow or flaky | at age |
| `LocalDiskFull`, `LocalIo`, `LocalFsyncFailed`, `StagedLost` | staging cannot be written | at age |
| `SqliteBusy`, `ReadersHeld` | checkpoint blocked; sync and upload go on | never by itself |
| `WalMoved`, `WalIndexInconsistent`, `WalChangedUnderRead`, `ReadTxLost`, `NothingToSnapshot` | WAL cannot be read this tick | at age (see "Not verified") |
| The same faults on the maintenance task only | compaction, retention | never |
| `ShutdownDeadline` | drain ran out | n/a |

### Unreachable remote

| When | hold-start | stop-app |
|---|---|---|
| Mid-run | 503 | app stopped at age, after the probe at the bound fails |
| Boot, timeouts or 5xx, nothing known unreplicated | **start the app** | start the app; the clock starts at the first commit |
| Boot, the store refuses (`StoreDenied`) before any listing | **held until the first listing succeeds** | same |
| Boot, staged files pending and the oldest is past the bound | start the app | held after one failed probe, until caught up |

**A boot with the remote down and nothing unreplicated does not hold the app.** Holding would turn every S3 incident into an outage of every node that restarts during it (deploy, crash, autoscale), for no data at risk: nothing is unreplicated yet. The boot doubt (BOOT-22) is not skipped by starting: the engine uploads nothing until its listing succeeds and the rule is judged, and if it then stops, that is trigger 1.

**The boot bound is the running bound, on the same clock.** The clock is the commit time of the oldest unreplicated commit, taken from the oldest pending staged file, so a restart does not reset it. A clock that starts at process start would let a crash loop run the app forever in slices shorter than the bound.

### Default bound and the napkin math

- Retry schedule: 1, 2, 4 ... s, capped at 300 s (`MAX_BACKOFF`). Failures land at 1, 3, 7, 15, 31, 63, 127, 255, 511 s after the first.
- S3 blips (5xx, timeouts, throttling): seconds to tens of seconds. Short incidents: a few minutes. Regional outages: 1 to 7 hours, about once in several years per region (us-east-1 2017-02-28 about 4 h; 2021-12-07 about 7 h). From memory, not looked up.
- What is at risk during an outage: the unreplicated writes, lost only if the local disk also dies in that window. At a 1 to 5 % annual loss rate for a host or volume, one hour carries about 1e-6 to 6e-6. A 4 h outage: about 2e-5 chance of losing up to 4 h of writes.
- Against that, `stop-app` at a 10 min bound makes a 4 h S3 outage a certain 3 h 50 min application outage. That trade only suits an operator whose loss cost is more than about 10,000 times their downtime cost per hour. So: **`stop-app` stays opt-in, and `hold-start` never acts on unreachability.**
- **Default `max-unreplicated-age`: 10 minutes.** It clears every blip and short incident and both retries before it (255 s, 511 s). At 100 commits/s it caps exposure at about 60,000 commits. Minimum accepted: 5 minutes (the backoff cap), so one capped retry always fits.
- **Probe at the bound.** Backoff alone would trip falsely: the remote heals at 260 s, the next retry is at 511 s. Before tripping, the engine makes one attempt at once; only its failure trips. This also makes a wall-clock jump harmless: with a healthy remote the probe succeeds.
- No byte bound proposed. Staging growth already ends in `LocalDiskFull`, which is covered by age.

## 2. The false-stop guard

Properties, judged by an oracle outside the engine (the simulator knows each commit's time and what the bucket truly holds):

- **P1, no false trip.** A trip at t implies: the engine is stopped with an unsafe cause, or (true age at t >= bound, and an upload or sync attempt failed inside the probe window before t).
- **P2, no missed trip.** True age >= bound + slack (one probe timeout plus one tick) implies tripped, under `stop-app`.
- **P3, clearing.** A trip for age clears within slack of age reaching 0. A trip for an unsafe stop never clears without an operator operation.
- **P4, enforcement.** No application commit is acknowledged while tripped (library), and the child is not running (`-exec`).
- **P5, restart.** Kill and restart at any point changes neither P1 nor P2: the clock survives.

Fault classes that must never trip (asserted over every sim run, plus one named scenario each):
- any store, staging or WAL fault that heals inside the bound, including throttling with a long `retry_after`;
- crash, kill, clean restart, power loss with an honest disk;
- Go handover in both directions with a shared meta directory; wiped staging where the WAL vouches; `take-over`; restore then start with the record;
- lagging listings; maintenance-only faults; a blocked checkpoint (`SqliteBusy`, `ReadersHeld`) while uploads flow;
- clock steps in either direction;
- remote down (timeouts, 5xx, throttling) at boot with nothing unreplicated;
- a denial after the first successful listing, inside the bound;
- a boot denial that heals (clock syncs, role propagates): the hold must release by itself.

Planted bugs (each must be caught unaided):
- trip on the first failed PUT;
- never trip;
- trip on `Stalled` (8 failures) instead of age;
- clock from process start (a crash loop never trips: P2, P5);
- clock from the last failure instead of the oldest commit;
- no probe at the bound (trip after a heal during backoff: P1);
- trip on a maintenance-task fault;
- no clear after catch-up (P3);
- hold the start on an unreachable remote;
- treat a boot timeout as a refusal;
- keep holding after the first listing succeeds;
- do not hold on a boot refusal;
- start the app over a kept stop record.

## 3. Enforcement

**Library.** `app_connection` installs a commit hook that reads one shared flag; while tripped it returns non-zero, SQLite turns the COMMIT into a ROLLBACK and the application gets `SQLITE_CONSTRAINT_COMMITHOOK` (531) on commit. Reads keep working. The replicator handle also reports the state, so an app can check before writing. Limits: it replaces the app's own commit hook (as `wal_autocheckpoint` already replaces its WAL hook), and it covers only connections set up through `app_connection`. Fallback that covers every process: the engine holds the write lock, and writers get `SQLITE_BUSY`; effective but the error misleads. Proposed: commit hook.

**`-exec`.** Boot hold: the child is not started; lsr stays up and answers 503. Running trip: SIGTERM to the child, the existing shutdown grace, SIGKILL, one last drain attempt, then lsr exits with a code of its own (not Go's 1), so a supervisor can tell "stopped for safety" from a crash. On the next start the boot rule holds the child until the cause clears, so a restart loop cannot run the app.

**Plain sidecar.** lsr cannot reach the app. It stays up with 503 and does not exit: an exit invites a restart that helps nothing. `stop-app` without `-exec` should be refused at config check, not silently downgraded.

**Clearing.**
- Unreplicated too long: automatic. Library: writes are accepted again once age is 0 (not merely under the bound, to avoid flapping). `-exec`: a held child is started once age is 0.
- Unsafe stop: an operator action (`lsr take-over`, fixing the cause), then a restart. Never automatic.

## 4. New user-visible strings (proposals, each needs approval)

- Health cause name for trigger 3, proposed `not_yet_reachable`, and its log line "start refused: {why}".
- Config key `max-unreplicated-age`, default `10m`.
- Health and stop cause name for trigger 2, proposed `unreplicated_too_long`.
- A health field or metric saying the app is held or refused, proposed `lsr_app_held{db,cause}`.
- Log lines, house format: "start refused: {why}", "commit refused: {why}", "app stopped: {why}".
- The `-exec` exit code and its line in the exit table.
- The config-check refusal for `stop-app` without `-exec`.
- Library: the error or state name the app reads.

## Decided (delegated to Fable by the owner, 2026-11-22; open to owner review)

1. **Boot refusal holds the app** (trigger 3). A refusal before any listing means nothing shows this configuration ever worked. It releases by itself, so a false hold costs a delayed start. Cost: a restart during an identity outage or with expired keys keeps the app down until fixed.
2. **Trigger 2 stays opt-in under `stop-app`.** The default never stops a running app.
3. **An unsafe stop while running trips `stop-app` at once.** A stop is terminal; waiting for the first unreplicated commit only delays the same outcome.
4. **After a running trip under `-exec`, lsr exits** with its own code. The supervisor owns restarts; the next boot holds the child. See #41 for the code.
5. **No write-lock enforcement for plain sidecar in v1.** `stop-app` without `-exec` is refused at config check. `SQLITE_BUSY` would mislead whoever debugs the app.
6. **`lsr take-over` does not need the daemon stopped.** The takeover record is its own file in the meta directory, written by rename, read at boot only, as #110's stop record is. The daemon must be restarted to act on it. It moves into the journal when the journal can take a second writer or the command goes through the daemon.
7. **10 minutes stands; no byte bound in v1.**

## As built (2026-11-22)

What the implementation settled where this design left a choice, or departed from it; each is a decision entry open to review.

- **The open question below is answered:** health's `unreplicated_since` did see WAL changes a sync fault hid (it reads the committed end on its own), but it restarted with the process, lived where the simulator could not judge it, and read zero while a snapshot was owed after a TRUNCATE. The clock now lives in the engine (`crates/core/src/actor/lag.rs`) and feeds health.
- **Where the clock cannot survive a restart** (D32): a boot that deletes pending files, unstaged WAL frames, a process not yet booted. The row "Boot, staged files pending and the oldest is past the bound: held after one failed probe" is therefore only met once the boot has listed.
- **What counts as lag** (D33): commits of the application; engine writes and doubt snapshots taken while caught up do not. Found by the false-trip judge.
- **When the boot is judged** (D34): after the first listing and the boot's own snapshot; until then the gate is pending.
- **Early trips and the probe** (D35): a trip needs a failure at or after the bound; a forward clock step with a failing store may trip early.
- **The setting's place:** `if-replication-stops` and `max-unreplicated-age` sit under `litestream-rs:` with the other Rust-only keys (provisional).
- **The `-exec` exit code:** 4 (provisional, #41).
- **P2, P3 and P5** are judged in named simulator runs, not over every run: the judge's truth is an upper bound on age, exact enough to rule out a false trip and too lenient to demand a trip.

## Not verified

- Whether `unreplicated_since` (`crates/replica/src/thread.rs:133`) is set when the WAL changed but a sync fault kept the engine from reading it. If not, WAL-read faults hide data at risk from trigger 2 and the clock needs a second source (WAL mtime or first failed sync).
- The exit table in `crates/cli/src/run.rs` was not read for a free code.
- Outage durations and loss rates are from memory.
