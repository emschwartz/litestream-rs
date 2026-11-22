# Behaviour: what the replicator does, case by case

Authored by Claude (claude-fable-5-2).

Version 3. What changed from version 2 is listed at the end of section 7.

**Scope, stated plainly.** Sections 3 to 5 account for every circumstance the code distinguishes: every variant of its closed enums has a row. That is not every circumstance the world produces. Circumstances the code does not distinguish (a paused process, two processes on one staging directory, a volume snapshot put back) are in section 2 and in the `WORLD` rows of section 3, most of them marked untested or unknown. The list of circumstances is open and will stay open; section 7 says what would shrink it.

**Read at** `origin/main` 50fb763; the rows #104 changed were read again at dd1d89b. Paths are under `crates/` unless they start with `docs/` or `scripts/`. Nothing was built or run to write it: every statement is a reading of the code, of a named test, or of a named report. Code is cited by file and symbol, never by line. Everything cited is a path in the repository, a test name, a commit, or a pull request or issue number on emschwartz/litestream-rs. Two measurements from earlier drafts were cut because their reports are not in the repository.

**Evidence marks.** A row's Mark column says what a test has shown, not what the row claims. Three marks can be earned, and they are separate:

| Mark | Earned when |
|---|---|
| **D** detected | A named test produces the circumstance, or injects its error, and asserts the response and the signal the row states. Says nothing about any restore |
| **S** survived | A restore is judged correct while the circumstance still holds |
| **R** recovered | A restore is judged correct after the circumstance cleared, or after the engine's one-time decision (a snapshot, a continue) took effect |

A row marked "D R" has no evidence about restores made during the fault. For a circumstance that is a single decision and not a lasting condition (most boot rows) S has no meaning and its absence is not a gap. For a lasting condition (credentials revoked, a full disk, a second writer) the absence of S is a gap: almost every simulator scenario injects one error at one call and judges the restore only after recovery (`injected` in `sim/src/scenario/faults.rs`). The random swarm does restore at arbitrary moments, including while rate faults are active, but no row can cite a swarm run as its evidence, so none earns S that way. Lasting faults now have a rig: a hold (`Op::Hold`, `sim/src/run/hold.rs`) keeps a fault in place for 2 to 11 ticks while the app commits, judges the raw replica with both judges in the middle and at the end, and restores through the fault where a restore meets it. The written histories in `sim/tests/sim/survival.rs` are what the S marks cite. A hold of WAL damage is judged against a model that adopted the damage, so it earns no S for the WAL rows.

Rows that earn none of the three carry one of these instead:

| Mark | Meaning |
|---|---|
| **code only** | Read in the code. No test asserts it |
| **assumed** | Taken from outside the project and not observed here |
| **untested** | Believed from reading; nothing exercises the circumstance |
| **unknown** | Nobody knows what happens |
| **known wrong** | A defect is open. Section 6 has it |
| **accepted, undetected** | A stated limit of the design |

Evidence is cited as: a test name with its crate ("sim" is `sim/tests/sim`, "core" is a unit test in `core/src`, "compat", "sqlite", "history", "cli", "replica", "local", "config" likewise); oracle `X`, a violation the simulator's judge raises on any seed (`Violation` in `sim/src/oracle.rs`); judge `X`, an anomaly of the history judge, which reads only restored content and a commit log (`Anomaly` in `history/src/judge.rs`); plant `X`, a defect kept as a patch in `docs/planted-bugs.toml` and re-planted nightly.

**Tally of the 157 rows in section 3** (counted by script from the tables; a row with a tested half and an untested half is counted by its tested half).

| Mark | Rows |
|---|---|
| D R | 52 |
| D | 50 |
| D S | 7 |
| D S R | 4 |
| S | 1 |
| untested | 18 |
| code only | 8 |
| unknown | 10 |
| known wrong | 5 |
| accepted, undetected | 2 |

**Changes in review.** Main is described as it is. Where an open branch changes a row, the last column names it:

| Tag | Branch | What it changes |
|---|---|---|
| #96 | `claude/listing-trust` | Level-0 uploads become conditional (`If-None-Match: *`); after every listing the engine reads past the listed end before trusting it; a listed tail is read twice before it is judged torn; a retried multipart Complete is settled by a HEAD; the simulator's listings can lag |
| #86 | `claude/checker-hygiene` | A per-cause snapshot counter in health and `/metrics`; a simulator oracle that fails a snapshot no injected fault justifies; canaries for the planted-bug runner |
| #89 | `claude/rewound-snapshot` | A staged snapshot of a rewound database survives a restart; a pinned position follows a new WAL header over a generation with no frames |
| #97 | `claude/crash-loop-cost` | Boot removes a killed process's scratch files and aborts its multipart uploads; prune and maintenance get a share of ticks while uploads are backlogged |

A planned engine journal is not described here. It is a design, not behaviour.

---

## 1. Guarantees

Each guarantee is stated as something an outside observer can check, the conditions it needs, the rows that count against it, and its evidence with seeds and sizes. G0 and G7 are liveness and health properties: without them an engine that uploads nothing would satisfy the rest.

| Guarantee | Open on main | Counted against it |
|---|---|---|
| G0 a restore of the latest state succeeds and is not older than what health reported | Yes | UP-11 (unseen case), MAINT-11, D2 |
| G1 a successful restore is a committed state | Hazards, none with a test that fails on main in the gate | BOOT-19 (expected-fail test, ignored), BOOT-18 (shown on #96's tree only), RUN-12 under the default response |
| G2 latest does not go backwards unreported | None shown on main | The exceptions' signals can be missing after a bucket rollback (D17, CHK-4) |
| G3 as-of returns nothing begun after the instant | None since #98 | Checked by the simulator's per-read clock since C7 was fixed (RUN-25, RUN-26) |
| G4 lag | No tested bound | N1 |
| G7 health is not clean while replication is halted | Yes | N1, D2 |

Version 0.0.0: these are what the code is built to deliver.

### Assumptions

| # | Assumption | State on main | What happens when it is false |
|---|---|---|---|
| A1 | An fsync that returns success made the bytes durable | Assumed | A power loss can rewind the database below the replica. Detected in part (BOOT-9); see G2 |
| A2 | SQLite's file locks and shared memory work on this filesystem | Assumed. No self-test | The read-lock proofs (RUN-7, CKPT-1) prove nothing. **unknown** on NFS, virtiofs, gRPC-FUSE |
| A3 | Only SQLite writes the database file and the WAL | Partly checked | A foreign write to the WAL is caught by frame checksums (RUN-4). A foreign write to a database page is caught only if the engine has seen that page before (RUN-12) |
| A4 | One replicator process writes to a replica prefix, and one process uses a staging directory | Not enforced. No lock exists on either | A second writer on the prefix is detected from a listing, late (UP-10, UP-14), and not at all at boot (BOOT-22). Two processes on one staging directory are not detected (WORLD-1) |
| A5 | The store returns the bytes it acknowledged, or bytes that fail to decode | Checked per file | Content-MD5 on every PUT and part, and a full LTX decode on every read. A different valid file under the same key is not caught (L1) |
| A6 | Every listing shows every object the bucket holds. This covers the engine's listings and the one listing a restore makes | **False on main for stores whose listings lag** | Engine side: a transaction id is reused for other content (BOOT-18). Restore side: an older state is returned as "latest" with exit 0 (WORLD-4) |
| A7 | The bucket is never replaced by an older copy of itself while the engine is down | **Not detected on main** | The older bucket is taken as the bucket (BOOT-19) |
| A8 | The database, WAL and staging directory are never together replaced by an older copy (a VM or volume snapshot put back) | Detected as `behind_remote`, not prevented | The latest state goes back to the snapshot's state (WORLD-3). A G2 exception |
| A9 | 64-bit checksums do not collide; a WAL `salt2` (32 random bits) does not repeat across the restarts being compared | Accepted risk | Not testable |
| A10 | The application runs `synchronous=NORMAL` or `FULL` | Not checked | Under `OFF` SQLite does not sync before a checkpoint and the engine's fsync of the WAL is the only barrier. **unknown** |

### G1. A successful restore is a state the application committed

**Statement.** Let `restore` return success for a target (latest, a transaction id, an instant). The published database is page for page the database as some commit left it. Conditions the operator owes: A3 (only SQLite writes the files), A4 (one writer), A5 (the store returns what it acknowledged). A6 and A7 are **not** conditions: a listing that lags and a bucket that was rolled back are faults the engine is meant to survive, and where it does not, that counts against G1 (BOOT-18, BOOT-19).

**What makes a violation silent.** When the replica holds a chain of files that each verify and together are wrong, `restore` cannot tell (REST-9): it returns exit 0. So G1 is kept or lost by the engine at upload time, never recovered at restore time.

**Counted against G1.**
- BOOT-19: across a restart after a bucket rollback, one transaction id is put twice with different bytes. Held by an expected-fail test, sim `a_rolled_back_bucket_never_gets_a_transaction_id_reused_across_a_restart_seed_0x484`, which is `#[ignore]` pending a durable record of what was confirmed. Its shrunk trace also needs a staging copy put back, so it is a double fault. No restore has been shown wrong from it.
- BOOT-18: a lagging boot listing leads to id reuse. Shown only on #96's tree; main cannot inject a lagging listing.
- RUN-12: under the default response a page no commit wrote is shipped. Deliberate, reported while it lasts, and still a restore that is not a committed state.
- WORLD-1 and WORLD-5 are suspected, not shown.

**Qualifiers.**
- A database-file page is trusted on its first read after the page table was emptied (`core/src/pages.rs`, "What it cannot check").
- "Committed" includes commits the local database later lost to a lying disk (G2).

**Engine-level evidence.**

| Evidence | Size and fault mix | Runs |
|---|---|---|
| Simulator fixed seeds: sim `seeds_1_to_8` through `seeds_57_to_64`, each judged by oracle `RestoreMismatch`, `WrongAnswer`, `RestoreRefused`, `CoversMidTx`, `UnwitnessedPut` and by the history judge through oracle `History(NotInHistory, Broken)` | 64 seeds of the default mix: commits, crashes, power loss (prefix law), store faults, clock jumps, staging damage, second writer, rollback. No lagging listings | Every commit (the gate) |
| Simulator regression seeds in `sim/tests/sim/fixed_seeds.rs` (about 30 named) | One history each | The gate |
| Simulator rotating seeds: sim `soak` | Default and swarm mixes for a time budget (`just sim-nightly`, 900 s) | Nightly. `#[ignore]` in the gate |
| Real engine, directory store: history `every_restore_of_a_file_replica_is_a_node_of_the_history` | **One seed (7), 60 steps, no faults, clean drained stops only** | The gate |
| Real engine, MinIO: history `every_restore_of_a_minio_replica_is_a_node_of_the_history` | The same driver against MinIO | **`#[ignore]`**. Run by `just test-s3`, which CI runs on pull requests |
| Real SQLite under the block crash law: history `every_real_restore_through_power_losses_is_a_node_of_the_history` | **One seed, 4 cycles** | The gate. The 50-seed version, `real_power_loss_trials`, is **`#[ignore]`** and run by hand |
| Real SQLite crash cycles: sqlite `crash_cycles_under_the_block_law_restore_committed_states` | **3 frozen seeds** | The gate. `crash_cycle_campaign` is **`#[ignore]`**, run by hand |
| Real binary on LazyFS: `scripts/lazyfs.py` | 300 cycles nightly; latest state only. The README on `claude/readme` reports 1000 cycles with 0 anomalies (at most about 0.3% per cycle at 95%, that fault mix only). Not re-run here | Nightly |

**Blind spots in that evidence.** Oracle `TxidReused` and the increasing-cover check used to be switched off for a whole run once any rewind, bucket rollback or lost file had occurred. They are now scoped (CHK-1): an id may cover a second commit only if the replica lost the first from outside, or a power loss cut the first before the second was put. Section 6, C1.

### G2. The latest state does not go backwards, except after a reported loss

**Statement.** Take two successful restores of the latest state, the second after the first, with A4 holding. The second is the same commit or a descendant, unless one of the events below happened between them.

| Exception | The engine's signal | Can the signal be missing? |
|---|---|---|
| The local database rewound below what the replica held: power loss on a disk that lied about fsync, a WAL cut by another program, or routine loss under a failed barrier | Snapshot cause `local_rewound`; `RecoverEvent::LocalRewound` | Yes, when the regrown WAL is byte-identical in the staged range. Then nothing differs either |
| Database, WAL and staging were all put back from an older copy (VM or volume snapshot; WORLD-3) | Snapshot cause `behind_remote` | No, but the same cause is raised for a wiped staging directory and for another writer's files (BOOT-6, BOOT-22). The signal does not say which |
| The WAL was lost and the database file is older than the replica (an operator restored the file; a WAL deleted before its frames were checkpointed) | Snapshot cause `wal_missing` or `generation_changed` | No, but these causes are also routine and harmless, so they do not identify a regression |
| Something removed the replica's newest files while the engine ran | Degraded cause `remote_regressed`, only if an ambiguous commit makes the uploader list (UP-11). Otherwise a retention hold, `ListingBehind`, which is not a degraded state | Yes (D17) |
| The restore's own listing lagged (WORLD-4) | None. The engine is not involved | Always |

After a rewind the replica still holds the lost branch. Its transactions stay restorable by id or instant until retention removes them.

**Counted against G2.** Nothing shown on main. Three simulator seeds once showed the latest state going back after a rollback and a restored staging copy; they pass since the witness commit b812e0f and are pinned: sim `a_staging_copy_put_back_after_two_rollbacks_never_restores_an_older_latest_seed_0x484`, `a_staging_copy_from_before_a_vacuum_never_restores_an_older_latest_seed_0x15c`, `a_staging_copy_put_back_after_a_truncated_wal_never_restores_an_older_latest_seed_0x385`. What remains from that family is id reuse, a G1 hazard (BOOT-19). G2 stays falsifiable only by the driver that injected the fault, because in two rows of the table above the engine's signal can be missing (C3).

**Engine-level evidence.** The same runs as G1, judged by judge `WentBackwards` (through oracle `History`) and oracle `LatestLostAck`.

**What that evidence does not show.**
- That every exception was reported. The simulator's history judge now requires the engine's own loss report before a recorded rewind explains a regression (CHK-3). A regression the driver excused (a bucket rollback) is a debt the engine must pay with a report later; about one in four is never paid, which is D17 (only an uploader listing raises `remote_regressed`). That is counted, not fatal (CHK-4). The real-engine history rig does not require the signal.
- `LatestLostAck` keeps acknowledgements across power loss now (CHK-2); a rollback voids those from the lowest id it uncovered.
- v1 cited history `a_recorded_rewind_explains_going_back_and_the_lost_branch_is_ahead` and `a_rewind_explains_nothing_below_its_target` here. Those feed a hand-built log to the judge. They show the checker can say no. They are not evidence about the engine.

### G3. A restore as of an instant contains no commit that began after the instant

**Statement.** A successful `restore` as of instant `t` contains no commit that began after `t` by the host's clock. This is the form the history judge checks since commit 938dbe7. The mechanism: the planner applies only files recorded as created strictly before `t`; a level-0 file is stamped after the committed read that bounds it (#98), so its stamp is not earlier than any commit it carries; a merged file carries its newest input's time. **G3 is an upper bound only.** Any older state satisfies it. How old the answer may be is governed by lag (G4), by outages, and by retention.

**Counted against G3.** Nothing on main. Until #98 the file was stamped with a clock reading taken before the committed read, and the history rig once saw a state committed after the instant (issue #99). Guarded now by core `a_commit_after_the_tick_began_is_not_stamped_before_it_landed`, `a_commit_before_the_checkpoint_barrier_is_not_stamped_before_it_landed` and plants `stamp-before-committed-read`, `seal-stamped-at-checkpoint-start`.

**Not bounded.**
- A commit is dated by when it was staged. Commits made while the engine was down or degraded are stamped at catch-up, so a restore as of an instant inside the outage leaves them out (D10). Bound: the outage. **untested**.
- An instant inside a merged file whose finer inputs are gone refuses with `AsOfInsideFile`; `--allow-earlier` returns the newest state provably before it.
- The times are the host's wall clock, never below its highest reading within one process (RUN-19), not across a restart (WORLD-13). A forward jump is believed; restores as of instants in that window are off by up to the jump. **untested**.

**Engine-level evidence.** oracle `AsOfTooNew`, `NotLatest`, `WrongCreatedAt` on the simulator runs listed under G1; sim `as_of_never_takes_an_upload_time_for_a_commit_time`; plants `time-provenance`, `time-provenance-sim`. The history rig restores at two instants per judged moment and applies judge `AsOfTooNew`.

**What that evidence does not show.** Under its per-read clock law the simulator reaches the window #98 fixed and kills the re-planted defect unaided (RUN-25, RUN-26; C7). v1 cited history `as_of_is_never_newer_than_the_instant` and `as_of_within_a_bound_is_the_node_live_then`; those test the judge on a hand-built log, not the engine.

### G4. Lag

**Design, not a tested bound.** With every task healthy, the store reachable, and writes slower than the upload path, a commit is fsynced and staged within one poll interval (1 s by default) and uploaded on the following ticks. No test asserts a wall-clock bound. **untested.**

**Tested.**
- After a stop that reports `Stopped::Drained { txid }`, a restore of the latest state is the local head. Evidence: the history rig applies judge `Lagging` with bound 0 at drained stops (one seed, see G1); core `drain_finishes_uploads`.
- A stop whose deadline passes reports what was left, counted high: core `drain_deadline_stops`, `a_zero_deadline_reports_unsynced_bytes_and_does_no_work`.

**What is lost if the host is destroyed.** Every commit after `replicated_txid`.

**How to see lag.** `lsr_unreplicated_age_seconds` is the intended measure. Until a booted engine shows the replica caught up (before a process's first boot, and while a rebuilt engine waits to boot) it counts from the start of that wait, never 0 (replica `the_lag_counts_from_the_start_while_the_first_boot_fails`). It still restarts from the process's start after every restart, so a crash loop understates lag (N1); alert also on `lsr_replicated_txid` failing to advance while the application writes.

**When degraded or retrying.** Lag is unbounded. The staging directory grows when uploads fail; the WAL grows when sync fails. No bound is enforced on either. **code only.**

### G0. A restore of the latest state succeeds, and is not older than what health reported

**Statement.** At any moment after the first upload, `restore` of the latest state succeeds, and the state it returns contains every commit up to the `replicated_txid` that health last reported. Exceptions: the G2 exceptions, and a store that is unreachable to the restore itself.

**Counted against G0.**
- UP-11, unseen case, and MAINT-11: after files are removed from the replica while the engine runs, a restore of the latest state refuses with `Gap` until the next scheduled snapshot, up to 24 hours, behind `/healthz` 200. No expected-fail test.
- D2: after a failed WAL fsync and a restart before the engine's TRUNCATE, frames that never reached disk can be uploaded, so `replicated_txid` can name commits the database then loses. Expected-fail tests (ignored): sim `wal_fsync_fails_then_restart`, sqlite `a_restart_after_a_failed_wal_sync_ships_no_unproven_frame`. Within one process the failure is handled (RUN-3).

**Evidence.** Oracle `LatestLostAck` (the latest restore is not older than the newest transaction the engine acknowledged) and oracle `RestoreRefused` on the simulator runs listed under G1. Acknowledgements survive power loss (CHK-2, which fixed C2).

### G7. Health is not clean while replication is halted

**Statement.** If the application has committed and no commit has reached the replica for longer than the retry cap (5 minutes) plus one poll interval, then at least one of these holds: `/healthz` is not 200; `Health.degraded` is not empty; `lsr_unreplicated_age_seconds` exceeds that time.

**Counted against G7.**
- N1: the age gauge restarts from the process's start at every restart, so a crash loop shorter than the alert threshold keeps it low. Before its first boot a process counts lag from its start, so a process that starts with the store unreachable no longer shows a clean age gauge; since #104 its boot task (the sync task) is also degraded as a stall from its 8th failed boot.
- D2: the WAL fsync failure itself is forgotten by a restart before the TRUNCATE.

D12 is closed by #104: every fault whose policy is to retry degrades its task as `Stalled(<fault>)` at the 8th failure in a row (`STALLED_AFTER` in `core/src/fault.rs`). From a 1 s interval the waits before the 8th failure sum to 127 s, inside the statement's 5 minutes. A failed WAL fsync is degraded at once (`wal_sync_failed`). A wait a maintenance level keeps never stalls; maintenance blocks no commit's way to the replica.

**Evidence.** core `every_retried_fault_stalls_and_no_stall_stops` (every `Fault` either is persistent, stalls, or is the end of a drain), core `a_retried_fault_that_persists_ends_degraded_as_a_stall` (eight injected `LocalDiskFull` on one task, then the degraded event and `Actor::degraded`). Both act on the policy and the actor; no test runs a halted engine for 5 minutes and reads `/healthz`. Mark: **D** for the routing, **untested** end to end. Tracked in issue #99.

Also in the simulator, end to end over the engine: a hold that keeps the replica from moving while the app commits, through at least twice the stall threshold of failed attempts, must end with some task degraded (oracle `HealthBlind`); sim `a_halt_long_enough_to_stall_is_reported_degraded` holds an upload outage, a full staging disk and revoked credentials for 40 ticks each; plant `stall-never-reported-sim`. Drawn holds (at most 11 ticks) rarely reach the threshold. `/healthz` and the age gauge (N1) are not read.

### Two further properties

**G5. Retention keeps a defined set of targets restorable.** Define the *retained set* of a listing at an anchor time as: the latest state; every transaction id from the last transaction of the first retained snapshot upward **that some retained file ends on**; and the restore point pinned while a task is degraded. Statement: after any delete job, or any subset of one, every target in the retained set that planned before still plans. What is *not* retained, by design: individual transactions older than the level-0 retention (5 minutes by default) once a merged file covers them. A restore to such an id refuses with `InsideFile`. So "point in time" inside the retention window means "to the granularity of the merged files that remain".
Evidence: the planner's self-check runs on every job (`retention` in `core/src/maintain/retain.rs`, "Self-check"), which is the planner checking itself; core `a_rule_that_breaks_the_newest_restore_is_caught_by_the_self_check`, `the_planner_keeps_its_promises_over_any_write_history`. Oracle `RetentionTooEager` checks ages against windows, not restorability. The simulator oracle does restore every id after deletes and accepts a refusal only "inside a kept compacted file", which is the closest thing to a judged G5. No test enumerates the retained set and restores each member. Rests on A6. Mark: **D**, by the engine's own planner.

**G6. A restore target is absent or complete.** The database is built in `<target>.restore`, fsynced, hard-linked into place, and the directory fsynced. An existing target is never overwritten. Evidence: cli `an_existing_output_exits_3_and_stays_byte_identical`, `a_gap_target_exits_2_names_the_hole_and_leaves_no_file`; local `a_target_created_during_the_restore_is_not_overwritten`, `a_failed_restore_leaves_no_target_and_no_scratch`. Power loss during publish is **untested**. Needs hard links.

---

## 2. Fault model

Scope has three values here. **In**: the engine is meant to keep G1 to G4 through the fault and a rig produces it. **Out**: excluded on purpose, with the reason given; an operator must prevent it. **Gap**: the fault is real, the engine is meant to cope or nobody decided, and no rig produces it. v1 filed several gaps as "out". "Rig" names what produces the fault in a test; "rig: none" is itself a finding.

The simulator can inject exactly these: `Op` (commit, spill, vacuum, shrink, other-process reader and checkpoint, crash, power loss, lying WAL sync, store fault, environment fault, clock jump, shutdown, snapshot request, staging damage, WAL damage, second writer, remote rollback, dropped created-at), `FaultShape` (error, error after publish, size or checksum mismatch, corrupt publish, flip a byte on read, fail at a rate), and `EnvFault`. A store fault fires at one call index or at a rate; **no shape holds a fault until a later step**, which is why lasting conditions have no S mark. Anything not in that list has no simulator rig.

### Local disk

| Fault | Scope | What the engine does | Rig | Notes |
|---|---|---|---|---|
| Power loss, honest disk. Each block written since the last fsync of its file keeps its old or new bytes independently; the file length is drawn separately | In | Rows BOOT-3 to BOOT-11 | Block crash law at 512 and 4096 bytes: `sqlite/src/testing/vfs.rs`, sim `block_law_campaign`, sqlite `crash_cycle_campaign`. The crash-cycle rig also judges restores while the loss holds: during the segment with the loss armed, with the power off before SQLite recovers, and after the reboot before the engine settles (sqlite `crash_cycles_under_the_block_law_restore_committed_states`; the campaign of 100 seeds of 12 cycles judged 1971, 568 and 568 such restores, 0 anomalies) | The law is taken from the ALICE model (Pillai et al., OSDI 2014). **assumed**: it has not been observed on real disks |
| Power loss on the real binary | In | Same | LazyFS (`scripts/lazyfs.py`): clear-cache after kill, torn-seq and torn-op on the WAL. It also restores while the loss holds, with the power off and nothing restarted (300 cycles from seed 1000: 166 such restores, 0 anomalies) | LazyFS passes metadata operations straight to the backing filesystem. A missing directory fsync cannot be seen |
| A lost directory entry: a rename or unlink not followed by a directory fsync | In | Commit of a staged file fsyncs the file, renames, fsyncs the directory; a delete removes its files and fsyncs each level directory it named, once per call (`store-fs/src/lib.rs`) | store-fs `a_crash_at_any_step_of_a_commit_loses_nothing_committed_and_shows_nothing_torn` and `a_crash_after_a_delete_never_brings_the_file_back` replay the steps and crash at every one (plants `staging-fsync-before-rename`, `staging-dir-fsync`, `delete-without-dir-sync`). The simulator treats a delete as durable at once, which the store now makes true | A directory the store did not create, made by an earlier crashed process and never synced, is the one window left (`store-fs` module doc) |
| fsync of the WAL fails | In, within one process | RUN-3 | sqlite `a_failed_wal_sync_stages_nothing_of_its_generation_and_snapshots_after_a_truncate`; sim `wal_fsync_fails` with the simulator's lost writeback | Forgotten by a restart before the TRUNCATE. **known wrong** (D2), expected-fail tests ignored |
| fsync of a staged file fails | In | RUN-15 | sim `fsync_fails_once` | |
| fsync of the database file fails inside a checkpoint | Unclear | Retried as local I/O (CKPT-8) | None | **unknown** whether SQLite's next checkpoint copies the pages again (D8) |
| Disk full | In | RUN-14 | sim `disk_full_on_staging` | Never escalates past "retrying" |
| Read error (EIO) on the WAL or database | In | RUN-13 | sim `eio_on_wal_read`; LazyFS dead-mount restores (`scripts/lazyfs.py`): a torn fault kills the filesystem under the running engine and the replica is restored meanwhile | |
| A disk that reports fsync success and keeps nothing | Out for prevention (no software can make it durable), in for detection | BOOT-9: detected only while the regrown WAL differs from what was staged | sim `power_loss_wal_rewind` (`Op::LyingWalSync`); plant `wal-barrier`; sim `a_power_loss_on_a_lying_disk_leaves_the_replica_restorable_while_it_lies` (a power loss inside a lying-disk hold, restores judged while the disk still lies) | No software can make such a disk durable. The replica is then ahead of the database (G2) |
| Bit rot in the database file | Partly | RUN-12 for a page the engine has seen. A page never seen is replicated as found | sim `db_page_flipped_under_sync` | A scrubber is not built |
| Bit rot in a staged file | In | BOOT-5, RUN-16 | sim `staged_byte_flip_before_upload`, `newest_staged_file_byte_flip` | |
| Torn write inside a block (old 512e disks) | Out. Reason: 4Kn and 512e-with-atomic-sector drives are the norm; SQLite's own WAL checksums catch a torn frame | n/a | n/a | The block law has no sub-block tear, so this is asserted, not shown |
| Filesystems where locks or shared mmap do not work (NFS, virtiofs, some container bind mounts) | **Gap.** SQLite itself is unsafe there, but users do it and the engine does not refuse | Not detected | None | **unknown**. A startup self-test or a documented refusal is missing (A2) |

### SQLite and other processes

| Fault | Scope | What the engine does | Rig |
|---|---|---|---|
| The application's own automatic or manual checkpoints | In | Counted and reported as a notice; they cannot pass the engine's read mark (RUN-22) | sim `app_autocheckpoints`; sim `rules_match_sqlite` compares the model with real SQLite |
| Another process checkpoints while the engine is down | In | BOOT-7, BOOT-8 | sim `app_checkpoint_while_down`; sqlite `the_apps_last_close_while_the_replicator_is_down_keeps_or_deletes_the_wal` |
| WAL deleted, truncated or its header torn | In | BOOT-7, RUN-5, RUN-6, RUN-9 | sim `wal_deleted_while_down`, `wal_header_torn`, `wal_truncated_externally_under_lock`, `wal_unlinked_mid_run` |
| WAL rewritten with other bytes in the same generation | In | BOOT-9, BOOT-10 | sim `wal_regrown_same_salts_diff_bytes`, `wal_rewritten_same_length`; core `a_rewound_tail_rewritten_with_the_same_last_page_is_not_continued` |
| A committed WAL frame flips a bit | In | RUN-4 | sim `bit_flip_in_committed_frame`, `bit_flip_in_committed_frame_healed` |
| Page size changes (`VACUUM` with a new `page_size`) | In | RUN-11, BOOT-15 | sim `vacuum_page_size_change`, `vacuum_page_size_change_and_back` |
| Journal mode changed away from WAL while the engine runs | In by SQLite's own rule | The engine's read transaction blocks it | sim `our_reader_blocks_leaving_wal_mode` |
| Journal mode changed away from WAL while the engine is down | In | Start puts the database back in WAL mode, or refuses with `NotWal` (CFG-4); the first sync is a snapshot | **untested** end to end |
| The database file replaced by another file at the same path | Partly | While running: the WAL's device and inode no longer match, `wal_unlinked` (RUN-6). While down: a snapshot, since the WAL cannot continue | replica `a_database_replaced_after_a_panic_is_degraded_and_not_reopened`. Inode number reuse making a different file look the same: **unknown** |
| A process that closes a file descriptor on the database inside the application's process | Out. Reason: POSIX semantics give a library no way to see or stop it; it is an embedding rule | POSIX locks for the whole process drop. Not detected | None. The embedding rules forbid it (`README`, library section) |
| `synchronous=NORMAL` (the usual WAL setting) and power loss | In | SQLite leaves committed frames unsynced until its next checkpoint, so power loss takes back acknowledged commits: SQLite's contract. The engine fsyncs the WAL before staging, so the replica does not run ahead on an honest disk; after the loss the boot continues or snapshots by the boot rows. The LazyFS rig sees the database lose acknowledged commits in most runs | LazyFS; sqlite `power_loss_at_each_wal_sync_loses_no_acknowledged_transaction` |
| `synchronous=OFF` | Out. Reason: SQLite itself promises no crash consistency, so there is no committed history to be faithful to | Not detected (A10) | None |

### The process

| Fault | Scope | What the engine does | Rig |
|---|---|---|---|
| Kill at any await point (`kill -9`, OOM) | In | Next boot recovers (section 3, boot rows) | sim `seeds_1_to_8` (crash operations in the fixed seeds); sqlite `a_writer_killed_mid_transaction_leaves_no_page_in_any_file`, `a_writer_killed_before_the_wal_index_update_is_never_replicated`; compat alternation with a kill at a random point |
| Panic inside a tick | In | RUN-20 | replica `a_panic_keeps_what_is_left_in_health_and_the_deadline_report` |
| Crash loop: the process dies every few seconds | In for correctness, **known wrong** for cost | BOOT-20 | LazyFS measurement only; no assertion |
| A restart right after a fault was reported | Not modelled | Retry counts, waits, degraded states and stops are all forgotten (BOOT-21) | None. **untested** |
| `fork()` of the host process | In | The child reports `Gone::Forked` and replicates nothing | **code only** (`replica/src/health.rs`) |

### The process, continued: faults with no rig

| Fault | Scope | What the engine does | Rig |
|---|---|---|---|
| Process paused (SIGSTOP, cgroup freezer, VM live migration, laptop sleep), also while holding the write barrier or the read transaction | **Gap** | WORLD-2 | None. **unknown** |
| Two processes on one database and one staging directory | **Gap** | WORLD-1. No lock exists | None. **unknown** |
| Failover with the old host still alive | **Gap** | WORLD-5 | None. **untested** |
| VM or volume snapshot put back | **Gap** | WORLD-3 | sim `restored_old_meta` covers staging alone |
| Filesystem remounted read-only; EROFS, EMFILE, EDQUOT | **Gap** | WORLD-16 | None. **untested** |
| The host application exits before a drain returns, or starves the replicator thread | **Gap** | WORLD-30 | None |

### The clock

| Fault | Scope | What the engine does | Rig |
|---|---|---|---|
| Steps back across a restart | **Gap** | The clamp is per process. Created-at times can run backwards across files (WORLD-13) | None. **untested** |
| Steps back | In | Engine time keeps its value, then runs at half the wall clock's rate until the wall clock catches up, after twice the step; each step back is reported once (RUN-19). Timers slow down for that window and never stop. Stamps stay on the wall clock, never below its highest reading | sim `clock_steps_back_10s`, `clock_steps_back_an_hour_mid_retry`; core `a_backward_clock_step_does_not_freeze_a_retry_wait`, `clock_back_clamped_once` |
| Steps forward | In for retention, out for as-of | Believed. Retention ages are measured from `min(now, newest created-at in the listing)`, so one bad reading deletes nothing the replica's own newest time would not (`core/src/maintain/retain.rs`). But files staged during the jump carry the future time, which raises that anchor | sim `clock_jumps_forward_an_hour` (no fault, every acknowledged commit restores); oracle `RetentionTooEager` judges against the simulator's own clock, so it shares the premise. Early deletion by the size of a forward jump is **untested** |
| The store's clock differs from the host's, as it affects retention | In, by construction | Ages never come from a listed last-modified time (`retain.rs`) | core `last_modified_matched_only_in_allowed_sites` |

### The object store

| Fault | Scope | What the engine does | Rig |
|---|---|---|---|
| Errors, 5xx, connection reset, timeout (60 s per request, `store-s3/src/config.rs`) | In | UP-2 | sim `store_flaky_5pct`; store-s3 `the_deadline_bounds_a_silent_server` |
| Throttling (429, `SlowDown`) | In | UP-3 | sim `store_429_retry_after` |
| A response body cut short | In | Treated as transient (`store-s3/src/classify.rs`) | store-s3 `a_truncated_body_is_never_a_whole_response`. No proxy rig drives a real client through it |
| A commit that fails after the object became visible | In | UP-6 | sim `commit_fails_after_publish` |
| The store acknowledges other bytes than were sent | In | UP-7, UP-8 | sim `size_mismatch_on_commit`, `etag_mismatch_on_put` |
| An object rots in the bucket | In, for the files the engine reads | BOOT-14, MAINT-3, MAINT-4, REST-5 | sim `remote_bitrot_on_list_read`, `corrupt_l0..l9_publish_then_power_loss` |
| Credentials revoked, a policy refusing, a missing bucket, a failed TLS check | In | UP-4 | sim `creds_revoked_mid_run`, `permission_revoked_mid_run`, `bucket_deleted_mid_run`, `tls_fails_mid_run` |
| Objects deleted by someone else (an operator, a lifecycle rule) | In for the newest files, partly for the middle | UP-11, UP-12, MAINT-11 | sim `remote_file_deleted_externally`, `bucket_rolled_back` |
| Bucket rolled back to an earlier version | Partly while running (seen only at an uploader listing). **known wrong** across a restart | UP-11, BOOT-19 | sim `Op::Remote(RollBack)` |
| A second writer on the same prefix | In, detected late | UP-10, UP-14, BOOT-22 | sim `second_writer_mid_run`, `maintenance_stops_on_another_writer_seed_0x50c` |
| Listings that lag the bucket | **Not modelled on main** | Trusted (BOOT-18, MAINT-12) | The lag law exists on #96 only. A consistency probe exists on #96 (`consistency_probe`); no result is in the repository |
| Multipart quirks: 200 with an error body, a retried Complete answered `NoSuchUpload`, ETags not in MD5 form | Partly | UP-13 | store-s3 fault transport. Fixed on #96 |
| Versioned buckets | **Gap.** Standard for backup buckets | A delete leaves a noncurrent version and a delete marker; the engine sees the key as gone, which is what it expects. A version restore by an operator is a rollback (BOOT-19, UP-11). Storage is not reclaimed by retention. All by reasoning | None. **unknown** (WORLD-24) |
| Object lock, or a policy that denies delete | **Gap** | Retention deletes fail for good, reported as `permission_denied` on the maintenance task (WORLD-23) | store-s3 `a_delete_refused_for_one_key_is_a_permission_denial` (the class only). **untested** in the engine |
| Objects moved to an archive tier, or under a disabled KMS key | **Gap** | An archived object is `store_refused`; a KMS failure goes by status (WORLD-22) | store-s3 `classify_denials_by_what_to_fix` (the archive class only). **untested** in the engine |
| AWS S3 | **Gap.** S3 is what the product is for; "never run" is not an exclusion | The S3 client is exercised against MinIO in CI and Tigris by hand (one 12-hour soak). Behaviour on AWS itself is **unknown** | None |
| Cloudflare R2 | **Gap** | A default concurrency of 2 is set for it; nothing else is known | None. **unknown** |
| A middlebox that returns a well-formed wrong body (captive portal, caching proxy) | **Gap** | A wrong body fails LTX decode, so a read is treated as a rotten object. MAINT-4 deletes a covered object after two failed reads; two reads through one bad proxy are not independent (WORLD-25) | None. **untested** |
| The endpoint resolves to a different, empty store; DNS failure | **Gap** | DNS failure is a transient error (UP-2). An empty store at boot looks like a new replica with staging kept (WORLD-11) | None. **untested** |
| Host clock skewed against the store | In, for one refusal | `RequestTimeTooSkewed` is `clock_skew` (WORLD-15) | sim `clock_skewed_against_the_store_mid_run` |
| Store quota exhausted; cross-region replication failover | **Gap** | **unknown** | None |

---

## 3. The behaviour table

Responses are one of: **continue** (no cost), **snapshot** (the next file is every page of the database), **retry** (the failing task waits and tries again; the wait doubles from 1 s to a cap of 5 minutes, `core/src/fault.rs`), **degraded** (a retry that is also reported as a persistent failure in `Health.degraded`, `/healthz` and `lsr_degraded`), **stop** (the engine does no further I/O until restarted; `/healthz` answers 503), **refuse** (a command or a start fails).

Five tasks keep separate retry waits and degraded states: sync, checkpoint, upload, prune, maintenance (`Task` in `core/src/actor/state.rs`). They are not independent. A boot fault holds all five. A checkpoint never runs without a sync that reached the committed end in the same tick. Maintenance runs only while uploads are idle, so an upload backlog or an upload fault holds merges, retention and the read-back (MAINT-13). A level-9 snapshot waits on the sync task.

### Health signals

| Signal | Where | Meaning |
|---|---|---|
| `Health.degraded[task]`, `/healthz` `degraded[]`, `lsr_degraded{task,cause}` | `replica/src/health.rs`, `sidecar/src/health.rs` | A persistent cause the task keeps retrying. Cause names: `credentials`, `permission_denied`, `clock_skew`, `bucket_missing`, `tls`, `store_refused`, `remote_corrupt`, `other_writer`, `remote_regressed`, `wal_corrupt`, `wal_truncated`, `wal_unlinked`, `wal_sync_failed`, `page_size_changed`, `database_page_mismatch`, `bug`; and for a stall (a retried fault that failed 8 times in a row), the fault's name: `store_transient`, `store_throttled`, `remote_object_missing`, `upload_ambiguous`, `upload_checksum_mismatch`, `staged_lost`, `local_disk_full`, `local_io`, `local_fsync_failed`, `sqlite_busy`, `readers_held`, `wal_moved`, `wal_index_inconsistent`, `wal_changed_under_read`, `nothing_to_snapshot`, `read_tx_lost` (`cause_name`, `stall_name` in `sidecar/src/health.rs`) |
| `Health.phase`, `lsr_db_state`, `lsr_stopped`, `/healthz` status | `sidecar/src/health.rs` | 200 while booting, running, draining, degraded or waiting for a file. 503 once any database stopped, failed to start, or lost its thread |
| `lsr_retries_total`, log line "retrying" | `sidecar/src/logging.rs` | A fault whose policy is to retry, for its first 7 failures in a row on one task. From the 8th the task is degraded as a stall (row above) and stays so until it succeeds. No retried fault stops the engine |
| `lsr_resnapshot_pending{cause}`, log "snapshot required", boot log `cause=` | `sidecar/src/health.rs` | The next file is a snapshot, and why |
| `lsr_unreplicated_age_seconds`, `lsr_replicated_txid`, `lsr_wal_bytes` | `Health` in `replica/src/health.rs` | Lag and local growth. The age gauge is 0 only when a booted engine shows the replica caught up; before that it counts from the process's start (N1) |
| `lsr_verify_failing`, `lsr_verify_failing_txid`, `lsr_verify_failing_page` | `replica/src/health.rs` | The scheduled verify found the replica restores something the live database never held |
| `lsr_app_notice`, `lsr_app_checkpoints_total` | `replica/src/health.rs` | The application ran checkpoints of its own. A notice, not a failure |
| `lsr_panics_total` | `replica/src/health.rs` | Panics caught at the thread boundary |

There is no per-cause count of snapshots taken on main. It is on #86 (`lsr_snapshots_total{db,cause}`).

### Fault index

<!-- enum: Fault -->

Every `Fault` variant (`core/src/fault.rs`), its policy (`policy`, `fault.rs`, one exhaustive match), and the rows that describe it. sim `every_fault_has_a_scenario_or_a_reason` asserts each variant has a scenario or a stated reason for none. Two variants are covered by a reason only: `Bug`, on the ground that oracle `BugFault` fails every run that meets it, and `WalSyncFailed`, whose scenario (sim `wal_fsync_fails`) holds under the default responses only, since under responses that stop the run's final restart meets D2 (`fault_reason` in `sim/src/scenario/faults.rs`). Having a scenario means D at most; see the marks.

Every policy written "retry" below also stalls: from the 8th failure in a row on one task the task is degraded `Stalled(<fault>)`, keeps the fault's own wait, and never stops (`Actor::stalled` in `core/src/actor.rs`). core `every_retried_fault_stalls_and_no_stall_stops` checks every variant.

| `Fault` | Policy | Rows |
|---|---|---|
| `StoreTransient` | retry | UP-2, UP-6 |
| `StoreThrottled` | retry, waiting at least what the store named | UP-3 |
| `StoreDenied(Denied)` | persistent: `StoreDenied` of the same kind | UP-4, WORLD-15, WORLD-23 |
| `RemoteObjectMissing` | retry | UP-12, MAINT-2 |
| `UploadAmbiguous` | retry | UP-7 |
| `UploadChecksumMismatch` | retry | UP-8 |
| `StoreRefused` | persistent: `StoreRefused` | UP-5, UP-13 |
| `RemoteCorrupt` | persistent: `RemoteCorrupt` | BOOT-14, BOOT-17, MAINT-3, MAINT-4 |
| `RemoteAheadOfLocal` | persistent: `OtherWriter` | UP-10, MAINT-10 |
| `RemoteRegressed` | persistent: `RemoteRegressed` | UP-11 |
| `StagedLost` | retry, then a snapshot on the second failed read | BOOT-5, RUN-16, RUN-17, RUN-18 |
| `LocalDiskFull` | retry | RUN-14, UP-15 |
| `LocalIo` | retry | RUN-13, BOOT-12, CKPT-8 |
| `LocalFsyncFailed` | retry | RUN-15 |
| `WalSyncFailed` | persistent: `WalSyncFailed` | RUN-3 |
| `SqliteBusy` | retry | RUN-13, CKPT-2 |
| `ReadersHeld` | retry | CKPT-3 |
| `WalMoved` | retry | RUN-13, RUN-8 |
| `WalIndexInconsistent` | retry | RUN-13 |
| `WalChangedUnderRead` | retry | RUN-13 |
| `NothingToSnapshot` | retry | RUN-13 |
| `ReadTxLost` | retry | CKPT-7 |
| `PageSizeChanged` | persistent: `PageSizeChanged` | BOOT-15, RUN-11 |
| `WalCorruptBelowCommitted` | persistent: `WalCorrupt` | RUN-4 |
| `WalTruncatedBelowCommitted` | persistent: `WalTruncated` | RUN-5 |
| `WalUnlinked` | persistent: `WalUnlinked` | RUN-6 |
| `DatabasePageMismatch` | persistent: `DatabasePageMismatch` | RUN-12 |
| `ShutdownDeadline` | stop: `Shutdown`, not configurable | RUN-24 |
| `Bug` | persistent: `Bug` | RUN-20 |

### Persistent causes and their configured responses

<!-- enum: PersistentCause -->

`Responses` (`core/src/fault.rs`) has one field per cause but `Stalled`, which is not configurable. `GO_LIKE` is the default: keep trying on every cause, stop on another writer. `STRICT` stops on every cause. Evidence that each preset routes every cause as it says: sim `under_strict_every_persistent_scenario_stops_with_its_cause`, `under_go_like_every_persistent_scenario_degrades_and_the_oracle_holds`. Most of those scenarios inject one error; they show routing (D), not behaviour under a lasting cause.

| `PersistentCause` | Default | What "keep trying" does (`action`, `fault.rs`) | Clears when |
|---|---|---|---|
| `StoreDenied(Denied)` | keep trying | Retries the same step. One response per kind: `credentials`, `permission_denied`, `clock_skew`, `bucket_missing`, `tls` | The step succeeds |
| `StoreRefused` | keep trying | Retries the same step | The step succeeds |
| `RemoteCorrupt` | keep trying | Retries the same step | The object verifies, or is repaired (MAINT-4) |
| `OtherWriter` | **stop** | With `ContinueAndInterleaveHistories`: drops all engine state, boots again, and snapshots after the other writer's newest transaction | n/a under the default |
| `RemoteRegressed` | keep trying | Forgets what the replica was known to hold, lists it again, continues from what it holds now | The relist succeeds |
| `WalCorrupt` | keep trying | Rescans. Nothing is staged past the bad frame | The frame verifies, or the WAL starts a new generation |
| `WalTruncated` | keep trying | Rescans | The WAL covers its committed end again, or a new generation |
| `WalUnlinked` | keep trying | Retries | Never by itself: the file is gone. An operator restarts the application and the replicator |
| `WalSyncFailed` | keep trying (`litestream-rs.responses.wal-sync-failed`) | Stages nothing more from the WAL generation the failed sync covered. The retry runs a TRUNCATE checkpoint in place of a sync, then snapshots (`actor/unproven.rs`). A busy or refused TRUNCATE is the same fault again | The TRUNCATE completes; the snapshot that follows clears the degraded task |
| `PageSizeChanged` | keep trying | Retries. Never writes the new page size | The page size returns, or a restart onto a new prefix |
| `DatabasePageMismatch` | keep trying | Ships the page as the file holds it; stays degraded | A verified WAL frame rewrites the page, the page table is emptied by a snapshot, or an operator calls `accept_database_pages` |
| `Bug` | keep trying | Drops all engine state and boots again after the wait | The fresh boot succeeds |
| `Stalled(<fault>)` | keep trying, not configurable, never a stop | Retries on the fault's own wait. Raised at the 8th failure in a row of a retried fault on one task | The task's next success |

While any task is degraded, retention pins the newest transaction the replica held when the first degradation began, and keeps its whole restore chain (`core/src/actor.rs`, `maintain/retain.rs`).

### Boot

A boot runs on the first tick of every process, and again after a rebuild. In order: list the replica and read back its newest level-0 file; sweep the staged files; witness the pending staged files against the live WAL; read the newest staged file; decide (`core/src/actor.rs`). A failure at any step starts the boot again from the top after the sync task's wait. Nothing is uploaded before the boot finishes.

<!-- rows: Boot -->

### Running

<!-- rows: Running -->

### Checkpoint

The engine runs every checkpoint itself, in the same tick as a sync that reached the committed end, and never without that sync's proof (`core/src/actor.rs`).

<!-- rows: Checkpoint -->

### Upload

<!-- rows: Upload -->

### Maintenance and retention

Maintenance runs only when every staged file is uploaded (`actor.rs`). Each job is planned from a listing made in its own tick.

<!-- rows: Maintenance and retention -->

### Restore

`restore` is a free function and the `lsr restore` command. It lists the replica once, plans a chain, applies it oldest first, and publishes only after every file verified. Exit codes: 0 success, 2 refused, 3 environment, 64 usage (`cli/src/run.rs`).

<!-- rows: Restore -->

### Verify

<!-- rows: Verify -->

### Handover with Go Litestream

The compat tests are `#[ignore]` in the gate and run by `just compat`, which CI runs on every pull request against the pinned Go build. Every Go 0.5.x release and upstream head run nightly (`compat-releases.yml`). Go 0.5.16 and later reads what this port writes.

<!-- rows: Handover with Go Litestream -->

### Fail-stop

`if-replication-stops` (`report`, `hold-start` by default, `stop-app`) and `max-unreplicated-age` (`docs/proposals/fail-stop.md`, decisions D14 to D22 and D80 to D83). Three things trip it and nothing else; what a trip does depends on the mode and on whether the application was released yet. The library refuses commits through a commit hook; `-exec` holds or stops the child; plain sidecar answers 503.

<!-- rows: Fail-stop -->

### Config and startup

<!-- rows: Config and startup -->

### Circumstances the code does not distinguish

These have no enum variant. WORLD-1 to WORLD-31 came from the first review and WORLD-32 to WORLD-40 from the second; none was found by the author unprompted, which says the next reader will find more. The response column is a reading of what the existing rows imply. Where the mark is **unknown**, the response is a guess and is labelled as one. The first five matter most.

<!-- rows: Circumstances the code does not distinguish -->

### The checkers themselves

A checker that cannot see a class of bug reads as green. These rows are the checkers' own blind spots that were closed, each backed by a planted bug that its test kills, and the one that stays open. The response is what the checker does.

<!-- rows: The checkers themselves -->
---

## 4. Snapshot causes

<!-- enum: SnapshotCause -->

Every `SnapshotCause` variant (`core/src/fault.rs`). A snapshot reads every page of the database under the read transaction and uploads it as one level-0 file; each compaction level later rewrites it. That every variant has a simulator scenario is asserted by sim `every_cause_has_a_scenario` and `every_scenario_runs`.

"Cheaper proof" is the author's analysis; none of the cheaper proofs is built, and none is in the repository as a design. "Needed" asks whether the doubt behind the snapshot is real.

| `SnapshotCause` | Fires when | Why a snapshot | Needed? | Cheaper sound proof | Rows |
|---|---|---|---|---|---|
| `FirstSync` | No staged file and no replica level-0 file to continue from | There is nothing to build on | Yes | None | BOOT-1 |
| `BehindRemote` | At boot, the replica's newest transaction is above the newest staged file's | The staged position is older than the replica | The doubt is real; the full upload is not needed | Read the replica's newest level-0 file and compare it with the live WAL, as boot already does when staging is empty. One small GET | BOOT-6, BOOT-22 |
| `NewestStagedFileUnreadable` | A staged file failed verification twice, or diverged from the replica's object, or had no witness token (boot refuses no more than the cap: RUN-18 holds the position instead) | The chain from the replica to the WAL has a hole in staging | After a divergence, yes. After a drop for verification it is taken only when recovery cannot continue from the newest file left (BOOT-5); mid-run (RUN-16) it is always taken, since a checkpoint may have removed the dropped frames and nothing judges that before the drop | Mid-run, judge the WAL against the newest file left before dropping, as boot does. The boot drop and the witness-cap case (RUN-18) no longer snapshot | BOOT-5, BOOT-13, RUN-16, RUN-17 |
| `WalMissing` | No WAL file, or a torn header | Commits after the position may have been checkpointed and are in no WAL | Yes. Not needed in one common case: a database just restored from this replica, which is snapshotted in full right after being downloaded | Compare the database's pages with per-page digests of the replica's tip and ship only the pages that differ. Needs local state that survives restarts | BOOT-7, RUN-9 |
| `GenerationChanged` | At boot, any salt change. Running, a restart that the read lock and `salt1 + 1` do not prove, or frames abandoned after the position | Commits written after the position and overwritten by the restart are in no WAL | The doubt is real in the sidecar case. Not when the crash fell between the engine's own checkpoint and its first file of the new generation | The page-digest compare above. This is the cause behind the measured crash-loop storm (BOOT-20) | BOOT-8, RUN-8, RUN-10 |
| `PrevFrameMismatch` | The frame before the position is not a commit frame, or the position is mid-transaction | The WAL is not the one the staged file came from | Yes | Page-digest compare | BOOT-10 |
| `LastPageMismatch` | The last page the staged file covers differs from the WAL | Same | Yes | Page-digest compare | BOOT-10 |
| `LocalRewound` | Same generation; the committed end or the file is shorter than the staged end, or the range was rewritten | The replica is ahead of the database, or holds other content. The snapshot puts the replica's latest state back in step | Yes | Page-digest compare ships only the reverted pages | BOOT-9, GO-4 |
| `CheckpointBoundary` | The engine's own TRUNCATE changed the WAL | A commit may have slipped between the last sync and the checkpoint | Usually not: nothing slipped in | Seal first, checkpoint with RESTART, and read SQLite's reported frame count to prove nothing was committed in between. Needs a spike on real SQLite | CKPT-4 |
| `OperatorRequested` | An operator asked | By definition | Yes | None | RUN-23 |
| `WalSyncFailed` | The TRUNCATE that follows a failed WAL fsync completed (RUN-3) | Commits the failed sync covered were never staged; the TRUNCATE wrote them into the database file, which the snapshot reads | Yes: those commits are in no WAL any more | None while the WAL is the only other copy | RUN-3 |
| `StagedUnverified` | At boot, pending staged files the live WAL does not vouch for, and recovery cannot continue | The files cannot be proven, so they are not published; the snapshot holds their end state | Not for files this process's predecessor wrote from a proven position: only the proof was lost with the process | Persist each staged file's token, chained to its predecessor. Needs a journal | BOOT-11, GO-3 |

**Cost today.** A snapshot costs one full read of the database, one full-size upload, its staged copy, and its share of each later merge. No rate limit, budget or backoff bounds how often any cause may fire. On main there is no per-cause counter, so the rate cannot be alerted on (#86 adds one).

---

## 5. Stop conditions

<!-- enum: StopCause -->

Every `StopCause` variant (`core/src/fault.rs`). A stopped engine does no I/O; every later tick returns the same cause (core `stop_is_terminal`). `/healthz` answers 503 and `lsr_stopped` is set. Nothing but a restart of the replicator clears a stop, and a restart forgets the stop (BOOT-21).

Under the default responses only two causes stop the engine: `OtherWriter` and `Shutdown`. The other eleven stop only when configured to (`Responses::STRICT`, or one field set to `Stop`).

| `StopCause` | Stops by default? | Why continuing is unsafe | What the operator does |
|---|---|---|---|
| `OtherWriter` | **Yes** | Two writers on one prefix give one transaction id two contents, and each one's retention can delete the other's files. Continuing would damage a history this engine does not own | Find the other replicator. Stop one. If the other's files should go, remove them. Restart. Note BOOT-22: a restart alone, with the other writer's files still there, continues above them |
| `Shutdown` | **Yes**, not configurable | A drain's deadline passed. The caller asked for an end | Nothing. The next start replicates what was left; the stop reported how much |
| `StoreDenied(Denied)` | No | Not unsafe. Stopping makes an access failure loud for an orchestrator | Fix what the kind names (the keys, the policy, the host clock, the bucket name, the TLS trust); restart |
| `StoreRefused` | No | Not unsafe | Read the status in the log; fix the request's cause; restart |
| `RemoteCorrupt` | No | Building on a corrupt object would make files that depend on it | Remove or replace the object named in the log; restart. A snapshot follows if the chain is broken |
| `RemoteRegressed` | No | The replica lost files it had acknowledged. Continuing accepts that loss and may reuse the vacated range | Decide whether the rollback was intended. Restart to continue from what the replica holds |
| `WalCorrupt` | No | Nothing past the bad frame can be trusted | Run `PRAGMA integrity_check`; checkpoint from the application so the WAL restarts; restart. A snapshot follows |
| `WalTruncated` | No | Same | Same |
| `WalUnlinked` | No | The file the engine reads is no longer the database's WAL | Restart the application's connections and the replicator |
| `WalSyncFailed` | No | The frames the failed sync covered may never reach the disk | Run `PRAGMA wal_checkpoint(TRUNCATE)` from the application before restarting: a restart alone trusts the WAL again (D2) |
| `PageSizeChanged` | No | A replica prefix holds one page size, in this port and in Go | Point the replicator at a new prefix. The old prefix keeps every state up to the change |
| `DatabasePageMismatch` | No | The database file holds bytes no WAL frame wrote. Shipping them replicates possible corruption | Inspect the page named in `/healthz`. Then either repair the database, or accept it (`accept_database_pages`) |
| `Bug` | No | The engine's own state contradicted itself | Report it with the `BugSite` from the log. A restart boots from disk and bucket |

Three other ways replication ends that are not a `StopCause`:

| Condition | Signal | Rows |
|---|---|---|
| The replicator thread is gone, or this process is a fork | `Phase::Gone(ThreadDied | Forked)`, `/healthz` 503 | RUN-21 |
| The database never started | `lsr_db_state` `failed`, `/healthz` 503 | CFG-4 |
| A retry that never succeeds (disk full, long readers) | "retrying", then from the 8th failure in a row a degraded stall naming the fault. No stop; `/healthz` 200 | RUN-13, RUN-14, CKPT-3 |

---

## 6. Known gaps

### Open defects

| # | Defect | Rows | Tracking |
|---|---|---|---|
| D1 | A crash loop takes one full snapshot per boot with no ceiling; maintenance and retention never run while processes die young; scratch files and open multipart uploads accumulate | BOOT-20, UP-15, MAINT-13 | #97 fixes scratch files and maintenance starvation. The snapshot per boot has no branch |
| D2 | A failed WAL fsync is forgotten by a restart before the engine's TRUNCATE, and the WAL is trusted again. Within one process it is handled since #104. Expected-fail tests (ignored): sim `wal_fsync_fails_then_restart`, sqlite `a_restart_after_a_failed_wal_sync_ships_no_unproven_frame` | RUN-3, BOOT-21 | Waits on the engine journal decision. Issue #99 |
| D3 | A bucket rolled back while the engine is down is taken as the bucket; freed transaction ids are reused. Expected-fail test: sim `a_rolled_back_bucket_never_gets_a_transaction_id_reused_across_a_restart_seed_0x484` (ignored) | BOOT-19, G1 | None open. Issue #99 |
| D4 | **Closed by #98.** A restore as of an instant could return a state committed up to one tick's read after the instant | G3 | Merged |
| D5 | A boot or relist listing that lags the bucket is trusted | BOOT-18, MAINT-12 | #96 |
| D6 | No conditional writes: a second writer or a reused id replaces an object instead of being refused | UP-14, GO-9 | #96 |
| D7 | A retried multipart Complete that already succeeded is reported as `store_refused` | UP-13 | #96 |
| D9 | **Confirmed by review.** Retry counts, waits, degraded states and stops reset on restart. The one stop the engine makes by default, `OtherWriter`, is undone by the default supervisor action: `boot_listing` names no newest staged file, so `recover_traced` returns `BehindRemote` for a boot and `Stop(OtherWriter)` only for a running position. The stop must be durable, or the boot must refuse | BOOT-21, BOOT-22 | Recorded as G3 in `docs/proposals/beyond-go.md`; issue #99. No branch |
| D10 | A commit is dated by when it was staged. After an outage, a restore as of an instant inside the outage leaves out commits made before that instant. The contract "what the replica held at t" is not stated anywhere a user reads | G3 | None. None |
| D12 | **Closed by #104.** A fault whose policy is a plain retry never became a degraded state. Now each degrades its task as `Stalled(<fault>)` at the 8th failure in a row, and a failed WAL fsync is its own persistent cause, `WalSyncFailed` | RUN-3, RUN-13, RUN-14, CKPT-3 | Merged |
| D13 | **Closed.** Past 4096 pending files a staged file got no token and was certain to be refused, which cost a snapshot and every restore point from there on. The engine now holds its position at the cap, and boot witnesses no more than the cap and stages the rest again from the WAL | RUN-18 | Closed |
| D14 | **Closed.** A boot that dropped an unreadable staged file snapshotted even when the WAL could regenerate it. It now continues when recovery does. The running twin (RUN-16) still snapshots | BOOT-5 | Closed |
| D15 | A staged snapshot of a rewound database is deleted by the next boot if the process died before uploading it, and taken again | BOOT-9 | #89 |
| D16 | **Closed.** A missing bucket and a TLS failure were reported as `credentials`. Each is now its own kind (N4) | UP-4 | Closed |
| D17 | **Confirmed by review.** A bucket rolled back while the engine runs is recognised as `remote_regressed` only if an ambiguous commit makes the uploader list. Otherwise the engine uploads above the hole and the only signal is a retention hold | UP-11, G2 | Issue #99 |

### Defects found by the review of version 1

| # | Defect | Basis | Rows | Tracking |
|---|---|---|---|---|
| N1 | **Half closed.** `lsr_unreplicated_age_seconds` read 0 until the first boot of a process completed; it now counts from the start of any wait no booted engine vouches for (`Progress::unproven`, `replica/src/thread.rs`). Open: it restarts from the process's start after every restart, so a crash loop understates lag | replica `the_lag_counts_from_the_start_while_the_first_boot_fails`; plant `lag-zero-before-boot`. The restart half is read in the code, not run | G4, G7 | Issue #99 for the restart half |
| N2 | **Closed.** A backward clock step held every timer for the size of the step: `Actor::clock` returned the old reading while the clock was behind. Observed before the fix (a one-second retry asked to wake an hour later after an hour's step back). Engine time now slews (`actor/clock.rs`) | core `a_backward_clock_step_does_not_freeze_a_retry_wait`, sim `clock_steps_back_an_hour_mid_retry`; plants `clock-step-back-freezes-timers`, `clock-step-back-freezes-timers-sim` | RUN-19, WORLD-14 | Closed, the replica thread's own waits included (WORLD-14) |
| N3 | No cross-process exclusion on the staging directory, and the `OtherWriter` detector is probably blind when two writers share it | No lock found by search. Consequence inferred | WORLD-1, A4 | Issue #99 |
| N4 | **Closed.** The `credentials` cause absorbed every 401 and 403, every S3 auth code including `RequestTimeTooSkewed`, a TLS failure and `NoSuchBucket`. Now five kinds (`StoreDenied`), an archived object is `store_refused`, and a batch delete refusing a key is `permission_denied` instead of transient. KMS codes are still classified by status | store-s3 `classify_denials_by_what_to_fix`; plant `denial-classes` | UP-4, WORLD-15, WORLD-22, WORLD-23 | Closed |

### Defects found by restores judged during a fault

Found on branch `claude/survival-tests` by the simulator's holds (`Op::Hold`, `sim/src/run/hold.rs`), which judge restores while a fault still holds.

| # | Defect | Rows | Tracking |
|---|---|---|---|
| S1 | **Fixed on the branch.** A restore of `Latest` or `AsOf` through a listing that lags published an older database and reported success | WORLD-4, G0, G2 | `claude/survival-tests`; plants `restore-trusts-listing`, `dry-run-trusts-listing` |
| S2 | **Fixed on the branch.** A restore over a listing that shows nothing answered "empty replica", which `--if-replica-exists` turns into a skipped restore with success: an application could start on an empty database over a replica that holds its history | WORLD-4, G0 | `claude/survival-tests`; plant `empty-listing-is-empty-replica` |
| S3 | **Fixed on the branch.** A boot with staging gone took a replica header that did not decode for no page size and snapshotted a new page size onto the prefix (swarm seed 0x13, under lying reads) | BOOT-15 | `claude/survival-tests`; plant `replica-page-size-unread` |
| S4 | Open, not triaged to a fix. Swarm seed 0xb9, shrunk to 13 operations (two second writers, a corrupt publish, a checksum-mismatch upload, redone commits): `Latest` goes back from one node to an older one with no rewind recorded, while the oracle passes. Matches the class of D9 (a boot that snapshots over another writer's files) | BOOT-22, G2 | Handed to the second-writer work |
| S5 | **A checker defect, fixed on `claude/survival-followup`.** Swarm seed 0x70f: the compaction byte budget counted a rolled-back level-1 file beside the file that replaced it at that level, 142 bytes over. No engine cost | MAINT | sim `a_rolled_back_output_replaced_at_its_level_counts_once` |
| S6 | Lasting ENOSPC on staging (a transient `StorageFull`, as the directory store reports it) is retried as `LocalIo`, not `LocalDiskFull`; only `ScratchExhausted` maps to `LocalDiskFull` | RUN-14, D12 | Handed to the health and classification work |

### Blind spots in the checkers

These are defects in the evidence, not in the engine. Each weakens a guarantee's claim to be checked.

| # | Blind spot | Effect | Tracking |
|---|---|---|---|
| C1 | Oracle `TxidReused` and the increasing-cover check are disabled for the whole run once any rewind, bucket rollback or lost file has occurred (`covers` in `sim/src/oracle.rs`: `rewound` is true if any snapshot note is rewound, the model rewound, or any file was lost) | The runs where id reuse happens (BOOT-18, BOOT-19) are the runs where the check is off. G1's simulator evidence does not cover id reuse after a rewind | Fixed: scoped to the commits a power loss cut or the replica lost (CHK-1) |
| C2 | Oracle `LatestLostAck` forgets every acknowledgement on every power loss, honest disk or not (`note_rewound`, called from the power-loss path in `sim/src/world.rs`), and on every rollback | G2's simulator evidence cannot fail on a regression after any power loss. The history judge still applies, by content | Fixed: acknowledgements survive power loss (CHK-2) |
| C3 | The judge excuses a regression on the driver's record (`history.power_loss`, `excuse_regression`), never on the engine's signal | "Reported" in G2 was unverified | Fixed in the simulator: judge `Unsignaled` (CHK-3); driver excuses counted when unpaid (CHK-4) |
| C4 | No fault shape holds until a later step; scenarios inject one error and judge after recovery | No row for a lasting store or disk condition earns S | None. Fix: `FaultShape::Until(step)` and restores during the fault |
| C5 | The simulator's oracle restores over an unfaulted store handle | Restore-side faults (WORLD-4) are never judged | None |
| C6 | The real-engine history runs in the gate are one seed with no faults, and one seed of four power-loss cycles; the larger runs are `#[ignore]` | G1's outside-the-engine evidence is thin except nightly LazyFS | None |
| C8 | After WAL or page damage the simulator's model adopts whatever SQLite now reads (`adopt_local_state` in `sim/src/world.rs`) | The oracle could not fail on shipped damage | Fixed: oracle `CorruptionRestored` refuses the adopted state (CHK-6) |
| C7 | The simulator has one clock read once per tick | It could not produce the stamp-order defect #98 fixed | Fixed: a per-read clock law, drawn last in the mix (RUN-25, RUN-26) |

### Unknowns

| # | Unknown | What would settle it |
|---|---|---|
| D8 | What SQLite does after a failed fsync of the database file inside a checkpoint, and whether the engine's retry is then safe | A fault VFS that fails `xSync` on the database once during TRUNCATE, then a power cut, compared with SQLite's recovery |
| D11 | **Closed on `claude/staged-delete-durable`.** A staged file deleted without a directory fsync could return after power loss. The directory store now syncs each level directory after a delete, so it cannot (store-fs `a_crash_after_a_delete_never_brings_the_file_back`, plant `delete-without-dir-sync`); whether the engine would have tolerated it is moot. Cost measured: about 0.14 ms per delete call on ext4 | Closed |
| U1 | The outcome of a failed write-barrier release on real SQLite | CKPT-6. A fault VFS run |
| U2 | Behaviour on filesystems where POSIX locks or shared mmap are unreliable | A2. A startup self-test, or a documented refusal |
| U3 | Whether a directory fsync that fails after the rename, then succeeds on retry, can leave a staged file that vanishes later | Not modelled |
| U4 | Compaction correctness beyond synthetic inputs. core `restoring_the_snapshot_and_the_chain_equals_restoring_the_compacted_file` compares the output's pages with `expected_pages`, a last-writer-wins merge written in the test, which is an independent apply; the simulator's oracle also judges restores through merged levels. Not covered: inputs written by SQLite at scale, a decoder not shared with the engine, a differential against Go's compactor | A differential run against Go's compactor on Go-written inputs |
| U9 | WORLD-1, WORLD-2, WORLD-11, WORLD-24: outcomes guessed from reading | A rig that drives the real binary with a second process, pauses, an emptied prefix and a versioned bucket |
| U5 | How stale listings and reads are on Tigris and R2 | `just consistency-probe-tigris 10000` exists on #96. No R2 harness exists |
| U6 | Behaviour on AWS S3, R2, versioned buckets, and buckets with lifecycle rules | Never run |
| U7 | Whether inode reuse can make a replaced database look like the same file | Not examined |
| U8 | The wall-clock lag bound when healthy | G4. No test asserts one |

### Accepted limits

| # | Limit | Why accepted |
|---|---|---|
| L1 | No file certifies its predecessor. Every file is written without a running page checksum, as Go's default is, so a chain of individually valid files with wrong content restores without error (REST-9) | Decision D2 in `docs/PLAN.md`: Go's compactor rejects tracked checksums, and Go must stay a fallback |
| L2 | The LTX file checksum covers decompressed pages, not the compressed bytes. Two different byte strings can decode to the same pages and both verify | Go's format. The upload token carries a digest of the raw bytes for the engine's own files |
| L3 | The engine keeps no state of its own across restarts (no file Go does not read, decision D6) | The root of D1, D2, D3, D9 and of `StagedUnverified` after a plain crash |
| L4 | A lying disk makes the replica run ahead of the database | No software fix. Detected only while the regrown WAL differs (BOOT-9) |
| L5 | The simulator and the engine can share a wrong belief about SQLite or the disk | Reduced by the history judge, real-SQLite crash cycles and LazyFS; not removed |

### What the evidence does not establish

- Recall. In the one measurement, the simulator alone found 7 of 22 planted bugs with no test written for them; 13 were masked in that run (the description of #86).
- The crash law is a model. It has not been compared with what real disks leave.
- Nobody outside the project has reviewed the code or this document.

---

## 7. How this document stays true

Part of the rows-as-data scheme is built. The tables of section 3 are rendered from `docs/behavior.toml`, one `[[row]]` per row with every cell verbatim, into this file's template (`docs/behavior/template.md`, `just behavior-doc`), and `just check` fails when this file differs from what they render (`crates/compat/tests/compat/behavior.rs`). It also checks every evidence cell: each citation has the form `kind::`\`name\``, each `plant::` names a ledger entry, each `oracle::` and `judge::` a variant that exists, each test citation a test that `cargo nextest list` knows (or a simulator scenario), a row with no test carries a no-test mark, and an In review cell starts with a PR number, a branch tag or `None`. Turning it on corrected five citations (three helper functions cited as tests, one simulator test cited by the core test's name, one module path), two rows whose notes sat in the In review column, and BOOT-17, which cited a plant and no test. A `plant::` citation must be backed: the row also cites a test the ledger entry runs (same package, a test filter that matches), and `just evidence-fresh` requires one such test not to be ignored, since the nightly ledger run (`just planted-bugs`) runs no ignored test. That is what makes a cited plant mean "this row's test kills a bug of this class". The marks themselves are still typed by a person reading tests: the rule they follow is below.

### 7.1 The rule used for the marks in this version

- **Detected by** lists every cited test. A row has D only if at least one is listed.
- **Recovered by** lists only cited tests from the simulator, the Go compat harness, the history rig or the real-SQLite crash tests, because only those judge a restore on content. A core, cli, local, replica, config or store unit test never earns R. Where the test's circumstance is narrower than the row's (one injected error for "credentials revoked"), R is earned for the narrow case and the Notes say what is not shown.
- **Survived by** lists a test only when its fault is still in place at the judged restore, by reading its operation list. Five rows. In four the engine has stopped staging, so S means "what was uploaded before still restores". In one (UP-2) a 5% rate fault is active. No row has S under a store that is down, revoked credentials, a full disk, a second writer's files, or an object that stays rotten.
- A restore judged against a model that adopted the damage earns neither S nor R (RUN-12). Since C8 was fixed, such a restore fails the run (CHK-6).
- **No-test mark** is filled when none of the three is, or for the half of a row that has no test.

### 7.2 How the tables map to the data file

Every table in section 3 has the same twelve columns and converts one row to one `[[row]]` table:

| Column | TOML key | Form |
|---|---|---|
| ID | `id` | Stable. Never renumbered; a removed row leaves a gap |
| Circumstance and how it is detected | `circumstance` | Prose. Backticked enum variants in it become `variants` |
| Response | `response` | Starts with one of: continue, snapshot, retry, degraded, stop, refuse |
| Signal | `signal` | Backticked names from the health-signal table |
| Data at risk, and the bound | `at_risk` | Prose |
| Recovery | `recovery` | Starts with: Automatic, Operator, n/a, or **unknown** |
| Detected by | `evidence.detected` | A list of `crate::`\`test_name\`, then after a semicolon `oracle::`, `judge::` and `plant::` entries, which go to `evidence.checks` |
| Survived by | `evidence.survived` | A list of `crate::`\`test_name\` |
| Recovered by | `evidence.recovered` | A list of `crate::`\`test_name\` |
| No-test mark | `evidence.none` | One of the six marks, with an optional scope before a colon |
| Notes | `notes` | Prose. Never evidence |
| In review | `in_review` | PR numbers or branch tags |

The index tables for `Fault`, `PersistentCause`, `SnapshotCause` and `StopCause` keep their `<!-- enum: X -->` markers and their first-column variant names.

### 7.3 What the scheme must check, including the second review's corrections

1. **Closed worlds.** Each closed enum's variants, through a wildcard-free match, equal the `variants` across rows: `Fault`, `PersistentCause`, `SnapshotCause`, `StopCause`, `BugSite`, `RecoverEvent`, `PlanOutcome`, `RetentionHold`, `StartError`, `lsr_sqlite::Refusal`, `Stopped`, `Gone`, `RestoreError`, the CLI's `Exit`, `S3Code`. The simulator's injectors (`Op`, `MetaOp`, `RemoteOp`, `WalOp`, `EnvFault`, `FaultShape`) likewise against each row's `injector` or `rig: none`.
2. **Marks are derived from what the rigs observed, not typed.** The simulator and the history rig emit one evidence record per judged restore: row id, injector, whether the injector was active at that step, whether it had cleared, which judge ran, the verdict. The gate computes D, S and R from the records of a run. A test cannot earn S unless a restore was judged on content while its injector was active. The three evidence columns then become generated output and stop being input.
3. **Every citation resolves.** Each evidence entry is `package::test`, one per entry, found by `cargo nextest list`. An ignored test counts only with a `runs_in` recipe that a workflow invokes. Every other citation must be a path in the tree, a commit, or a PR or issue number; anything else fails.
4. **Staleness.** The file records `read_at`. The gate fails unless `read_at` is `origin/main`, or no commit since touches a cited symbol or test.
5. **Derived columns equal the code.** `response` equals `policy` and the default `Responses`; `signal` equals `cause_name`, `snapshot_name`, `stop_name`.
6. **Marks have owners.** A no-test mark needs an issue number. An `in_review` tag whose PR merged fails until the row is updated. This version would have failed that check on the tag for #98.
7. **Known wrong is an expected failure.** A known-wrong row names a test that fails on main, or carries "(no failing test)", which the gate counts and prints. Today two rows have such a test (BOOT-19, and RUN-3's restart half) and six rows or half-rows do not.
8. **Every citation can fail (nightly).** Each cited test is linked to a planted bug that it kills. The plant must change the function the row's detection names, or the response path; a patch elsewhere that happens to fail the test does not count.
9. **The cited test reaches the detecting branch (nightly).** Region coverage of the branch, not the function: every test reaches `policy` and `Actor::tick`.
10. **Sections 1 and 6 are generated.** The table at the top of section 1 and the tally below come from the data, so a sentence like "three of four have an open violation" cannot go stale by itself.
11. **A generic harness**, where a row has a simulator injector: run the injector, and assert the observed response, signal and restore outcome equal the row's.

### 7.4 What this still does not do

It does not find the circumstance nobody wrote down. The way to close the world is over the engine's interface to it, which is finite: every syscall it makes on the database, WAL and staging; every S3 operation; the clock; signals; config keys; the pragmas it reads. Cross each with a fixed set of failure modes (error, delay, wrong result, stale result, partial result, duplicate, reorder, lasting). Each cell is a row, an "impossible, because", or a gap. That inventory is not in this version. It is the largest thing missing from this document.

### Changes from version 2

- Read again at 50fb763. G3's violation is closed by #98. G2 has no violation shown on main; what remains is id reuse across a restart, moved under G1 with its ignored expected-fail test.
- Every citation is now a repository path, a test, a commit, or a PR or issue number. Five scratchpad reports are no longer cited and the figures that depended on them are cut.
- Evidence split into one column per mark, by the rule in 7.1. Corrected: RUN-12 (S removed), RUN-7, UP-1, CKPT-1, RUN-11 (R or S removed), UP-2 (S, not R), RUN-4 (R from the healed scenario only), MAINT-3, CFG-7, CFG-4; UP-4, UP-5, RUN-14, RUN-15, UP-12, UP-10 and BOOT-14 gain R for the narrow case their test shows, so the same test shape earns the same mark everywhere.
- G0 (a restore succeeds and is not older than health reported) and G7 (health is not clean while replication is halted) added, with UP-11, MAINT-11, D2, D12 and N1 counted against them. G1 no longer treats a lagging listing or a rolled-back bucket as an assumption.
- C8 added. U4 restated. WORLD-32 to WORLD-40 added. Line numbers removed.

### Changes for #104

- RUN-3 rewritten: a failed WAL fsync is the persistent cause `WalSyncFailed`, degraded by default, and the engine pays for it with a TRUNCATE and a snapshot; D R. Its restart half stays known wrong (D2), now with two ignored expected-fail tests. BOOT-21's in-review tag removed.
- D12 closed: every retried fault stalls at the 8th failure in a row. RUN-13, RUN-14, CKPT-3, WORLD-16, the health-signal table, the fault index and the persistent-cause, snapshot-cause and stop-cause tables updated. G7 no longer counts D12.
