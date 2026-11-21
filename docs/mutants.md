# Mutation testing

`just mutants <crate>` runs cargo-mutants 27.1.0 over `lsr-<crate>` with nextest, four jobs, the tree copied with
its `.git`, results in `target/mutants/mutants.out/`. It runs on demand, not in CI. Phase 2's exit criterion is that every
survivor in `ltx` and `core` is killed by a test or listed here with a reason. `ltx` meets that; `core` is part way
(see its section).

## Scope: which test-only code is mutated

Test-only helper code is mutated only where product correctness depends on it (the decision in issue #22).
A mutant in a test helper weakens a test rather than the engine, so it is worth a run and a triage only when
nothing else would notice the helper being wrong.

- **Mutated:** `crates/core/src/mem.rs`, the in-memory store. It is public, other crates test against it, and
  the simulator uses it as its reference model of an object store. `crates/core/src/conformance.rs`, the
  shared checks the real S3 and filesystem stores must pass: a check that stops checking would let a broken
  store through.
- **Excluded** in `.cargo/mutants.toml` (`exclude_globs`): `crates/core/src/source/fake.rs` with its
  submodules (the scripted WAL, database and checkpointer fakes) and `crates/core/src/restore/mem_sink.rs`
  (the in-memory restore target). They are checked another way:
  `crates/compat/tests/compat/live_matches_sim.rs` compares the simulator's SQLite model with real SQLite. The
  exclusion drops `cargo mutants --list -p lsr-core` from 1705 mutants to 1569 (102 in `fake.rs`, 34 in
  `mem_sink.rs`); `mem.rs` keeps its 24 and `conformance.rs` its 49. A run that wants them anyway passes
  `--no-config`.

## ltx

Every mutant not listed below is caught or unviable (does not compile). Tests added for the first run's
survivors: `tests/ltx/pins.rs`, the `tests` modules of `checksum.rs`, `encoder.rs`, `frame.rs`, `ids.rs`
and `page.rs`.

Tally (581 generated, about 7 minutes at four jobs): first run 29 missed, 477 caught, 68 unviable, 7 timeouts;
final run 3 missed (all equivalent, below), 503 caught, 68 unviable, 7 timeouts (all below).

### Equivalent (no input can tell the mutant from the original)

| Where | Mutation | Why |
| --- | --- | --- |
| `checksum.rs` `DbChecksum::toggle` | `FLAG \| (a ^ b)` becomes `FLAG ^ (a ^ b)` | Both operands carry bit 63 (the type only holds flagged values), so `a ^ b` has it clear and OR and XOR of the flag agree. |
| `ids.rs` `Txid::from_str` | `(raw << 4) \| digit` becomes `^` | The shift leaves the low nibble zero and `digit` is below 16, so the operands never overlap. |
| `index.rs` `put_uvarint` | `(v & 0x7f) \| 0x80` becomes `^` | `v & 0x7f` has bit 7 clear, so OR and XOR of `0x80` agree. |

### Detected by non-termination (cargo-mutants reports TIMEOUT)

Each makes a loop spin forever, so any test that reaches it hangs and the run kills it. That is a failing
test, not a survivor, but it is reported separately and slower than a catch.

| Where | Mutation | Loop that spins |
| --- | --- | --- |
| `decoder.rs` `Decoder::feed` | body replaced by `Ok((0, None))` | every caller's feed loop makes no progress |
| `decoder.rs` `Decoder::feed_raw` | body replaced by `Ok((0, None))` | same |
| `decoder.rs` `feed_raw` `self.unit.len() < need` | `<` becomes `==` or `<=` | a full unit is never processed, so no bytes are consumed |
| `decoder.rs` `ReadDecoder::next_event` `self.pos == self.len` | `==` becomes `!=` | the buffer is never refilled, or is refilled forever |
| `decoder.rs` `read_retrying` | match guard becomes `true` | every read error is retried forever |
| `index.rs` `put_uvarint` `value >= 0x80` | `>=` becomes `<` | the shift loop never ends for a multi-byte value |

## core

**Status: met.** The first run was stopped at 899 of 1463 mutants (about eight hours on four shared
cores). The second run, over both test packages, finished, and every one of its survivors is killed by a
test, equivalent or explained (see the two "Second run" sections and "Kept test helpers" below). The exit
bar, zero unexplained survivors, is met for `core`. Which test doubles are mutated is settled in "Scope"
above; the survivors of the excluded files are no longer listed.

The recipe needed one change before it could run at all: `core` has a guard test that lists the tracked files
with git, cargo-mutants copies the tree without `.git` by default, and so the unmutated baseline failed and no
mutant was tested. `just mutants` now passes `--copy-vcs true`.

Tally of the first run so far: 491 caught, 88 missed, 13 timeouts, 307 unviable. Of the 88 missed,
31 are now caught by new tests (each run again and confirmed caught), 1 is equivalent, 7 are in the store
conformance harness (now caught: "Kept test helpers" below), and 49 were left open; the second run caught or
explained every one of those it has reached.

This run executes `lsr-core`'s own tests only. The actor's widest tests are the simulator's scenarios in
`lsr-sim`, which a `-p lsr-core` run does not execute. A trial with `--test-package lsr-core --test-package lsr-sim`
reached two of the actor's survivors before it was stopped for time: it caught `Actor::request_snapshot` (since
also caught by a core test) and missed `actor.rs:573:13`. The second run executes both packages.

### Caught by new tests (31)

Tests added: `entropy_is_splitmix64` and `below_scales_the_whole_output` (`env.rs`),
`the_longest_backoff_is_five_minutes` (`fault/tests.rs`),
`with_created_at_keeps_every_file_and_gives_each_its_stored_created_at` (`inventory.rs`),
`debug_and_the_range_side_print_what_a_log_line_needs` (`layout.rs`), `the_chunk_is_one_mebibyte`,
`a_small_output_is_written_to_the_upload_once` and a lower bound on the measured peak in the memory-bound test
(`compact/tests.rs`), `the_failed_state_stays_at_the_threshold`,
`a_position_after_a_checkpoint_is_pinned_at_the_epoch_it_retook`,
`the_seal_takes_every_committed_transaction_whatever_the_cap` and the read lock assertion in
`a_passive_held_back_by_a_reader_is_not_restarted` (`checkpoint/tests.rs`), `a_first_boot_lists_each_level_once`
and `a_requested_snapshot_is_staged_on_a_quiet_wal` (`actor/tests.rs`).

| Where | Mutation |
| --- | --- |
| `actor.rs:335:9` | replace Actor::request_snapshot with () |
| `actor.rs:1190:52` | replace match guard confirmed.is_some() with true in boot_recover |
| `checkpoint.rs:218:62` | replace match guard !over_truncate(observed, cfg) with true in after_sync |
| `checkpoint.rs:386:9` | replace Protocol<'_, W, P, C, L>::read lock with () |
| `checkpoint.rs:386:29` | replace == with != in Protocol<'_, W, P, C, L>::read lock |
| `checkpoint.rs:398:13` | delete field cap from struct SyncConfig expression in Protocol<'_, W, P, C, L>::step |
| `compact.rs:68:31` | replace * with + |
| `compact.rs:68:31` | replace * with / |
| `compact.rs:304:9` | replace Compacted::peak_buffered -> usize with 0 |
| `compact.rs:304:9` | replace Compacted::peak_buffered -> usize with 1 |
| `compact.rs:488:9` | replace Input::held -> usize with 0 |
| `compact.rs:488:9` | replace Input::held -> usize with 1 |
| `compact.rs:549:9` | replace Merge<'a, S>::observe with () |
| `compact.rs:708:39` | replace >= with < in write_output |
| `env.rs:145:13` | replace fakes::SeededEntropy::next_u64 -> u64 with 0 |
| `env.rs:145:13` | replace fakes::SeededEntropy::next_u64 -> u64 with 1 |
| `env.rs:149:31` | replace ^ with \| in fakes::SeededEntropy::next_u64 |
| `env.rs:149:31` | replace ^ with & in fakes::SeededEntropy::next_u64 |
| `env.rs:150:32` | replace ^ with \| in fakes::SeededEntropy::next_u64 |
| `env.rs:149:39` | replace >> with << in fakes::SeededEntropy::next_u64 |
| `env.rs:150:32` | replace ^ with & in fakes::SeededEntropy::next_u64 |
| `env.rs:150:41` | replace >> with << in fakes::SeededEntropy::next_u64 |
| `env.rs:151:19` | replace ^ with \| in fakes::SeededEntropy::next_u64 |
| `env.rs:151:19` | replace ^ with & in fakes::SeededEntropy::next_u64 |
| `env.rs:151:28` | replace >> with << in fakes::SeededEntropy::next_u64 |
| `env.rs:159:13` | replace fakes::<impl Entropy for SeededEntropy>::below -> u64 with 0 |
| `env.rs:160:32` | replace >> with << in fakes::<impl Entropy for SeededEntropy>::below |
| `fault.rs:27:57` | replace * with + |
| `inventory.rs:112:9` | replace Inventory::with_created_at -> Result<Self, StoreError> with Ok(Default::default()) |
| `layout.rs:93:9` | replace <impl fmt::Display for RangeSide>::fmt -> fmt::Result with Ok(Default::default()) |
| `layout.rs:245:9` | replace <impl fmt::Debug for FileKey>::fmt -> fmt::Result with Ok(Default::default()) |

### Equivalent (no input can tell the mutant from the original)

| Where | Mutation | Why |
| --- | --- | --- |
| `layout.rs` `FileKey::from_str` | `(raw << 4) \| digit` becomes `^` | The shift leaves the low nibble zero and `digit` is below 16, so the operands never overlap. |

### In the store conformance harness (caught since)

`conformance.rs` is the suite every `ObjectStore` runs, built only for tests. A mutant here does not change the
engine: it makes the suite ask less of a store, and the stores in this repository are correct, so they pass the
weaker suite too. The first two runs left these seven; the suite's self-tests (`conformance/tests.rs`) now
catch them, see "Kept test helpers" below. Line numbers are those of the first run.

| Where | Mutation |
| --- | --- |
| `conformance.rs:28:9` | replace <impl fmt::Display for Failure>::fmt -> fmt::Result with Ok(Default::default()) |
| `conformance.rs:61:9` | replace <impl Wake for ThreadWaker>::wake with () |
| `conformance.rs:99:9` | replace Harness::fail_next_commit_after_publish -> bool with true |
| `conformance.rs:111:9` | replace MemHarness::listing_last_modified -> Self with Default::default() |
| `conformance.rs:128:9` | replace <impl Harness for MemHarness>::fail_next_commit_after_publish -> bool with false |
| `conformance.rs:379:8` | delete ! in commit_error_may_leave_the_object_visible |
| `conformance.rs:634:5` | replace exact_or_marked -> bool with true |

### Reported as TIMEOUT (13)

cargo-mutants reports a timeout when the tests stop finishing, which for these is a loop that no longer ends.
That is a failing test, not a survivor. The second run reports twelve of them again, at new line numbers, and
its timeout table names the loop each one spins; `retry.rs:205:9` is caught outright there.

| Where | Mutation |
| --- | --- |
| `actor.rs:383:27` | replace match guard now < prev with true in Actor::clock |
| `actor.rs:383:31` | replace < with > in Actor::clock |
| `actor.rs:1116:61` | delete ! in Actor::ready_work |
| `compact.rs:371:9` | replace Reader::refill -> Result<bool, CompactError> with Ok(true) |
| `compact.rs:382:16` | replace > with >= in Reader::refill |
| `compact.rs:396:25` | replace >= with < in Reader::step |
| `maintain.rs:457:72` | replace > with == in plan_level |
| `maintain.rs:457:72` | replace > with >= in plan_level |
| `maintain.rs:535:5` | replace created_after -> Result<bool, FileInfo> with Ok(true) |
| `restore.rs:508:16` | replace < with == in apply_file |
| `restore.rs:508:16` | replace < with > in apply_file |
| `retry.rs:205:9` | replace Core<S, P, E>::wait_after -> Option<Duration> with Some(Default::default()) |
| `store.rs:80:9` | replace ReadLen::get -> u64 with 0 |

### Second run: both test packages, survivors resolved

The second run is `cargo mutants -p lsr-core --test-package lsr-core --test-package lsr-sim --iterate`, so
the simulator's scenarios count as well as core's own tests, and mutants an earlier run caught are skipped.
Line numbers in this section are those of commit `ec5bc07`. This section covers the survivors it had
reported when the first batch was taken (the files `actor.rs` to `upload.rs` in its order); the next
section covers the rest.
Survivors of the first run that are not listed here were caught in the second run: the simulator's tests or
tests added since then kill them.

Tally of this batch: 45 missed and 13 timeouts. Of the 45 missed, 28 are caught by new tests, 9 are
equivalent, 1 is explained but not killed (it costs one listing), and 7 are the harness survivors already
explained above. The 13 timeouts are all explained below.
No survivor revealed a bug.

| File | Killed | Equivalent | Explained | Harness | Timeout |
| --- | --- | --- | --- | --- | --- |
| `plan.rs` | 4 | 6 | | | |
| `recover.rs` | 1 | | | | |
| `sync.rs` | 3 | | | | |
| `store.rs` | | 1 | | | 1 |
| `layout.rs` | | 1 | | | |
| `retry.rs` | 1 | | | | |
| `upload.rs` | 2 | | | | 1 |
| `created_at.rs` | 3 | | | | |
| `actor.rs` | 14 | 1 | 1 | | 3 |
| `conformance.rs` | | | | 7 | |
| `compact.rs` | | | | | 3 |
| `maintain.rs` | | | | | 3 |
| `restore.rs` | | | | | 2 |
| Total | 28 | 9 | 1 | 7 | 13 |

#### Caught by new tests (28)

Each was run again with `--file <file> --re '<file>:<line>:<col>'` and both test packages, and is caught.

| Where | Mutation | Test |
| --- | --- | --- |
| `plan.rs:441:41` | replace && with \|\| in explain_shortfall | `a_refused_snapshot_past_a_hole_is_reported_as_refused` (`plan.rs`) |
| `plan.rs:563:9` | replace AsOfPlanner::comparisons -> u64 with 0 | `comparisons_count_both_sorts_and_every_heap_step` (`tests/core/plan_lazy.rs`) |
| `plan.rs:563:9` | replace AsOfPlanner::comparisons -> u64 with 1 | same |
| `plan.rs:733:5` | replace depth -> u64 with 0 | same |
| `recover.rs:756:42` | replace match guard at_start with false in frame_state | `rule_9_at_boot_a_newest_staged_file_ending_at_the_wal_start_has_no_frame_to_miss` (`recover/tests.rs`) |
| `sync.rs:35:29` | replace << with >> (`DEFAULT_CAP`) | `the_default_cap_is_go_s_64_mib` (`sync/tests.rs`) |
| `sync.rs:38:27` | replace << with >> (`FLUSH_AT`) | `a_small_file_reaches_the_upload_in_one_write` (`sync/tests.rs`) |
| `sync.rs:231:28` | replace > with >= in clamp | `an_equal_clock_is_not_a_clamp` (`sync/tests.rs`); the existing proptest draws equal created-at times about once in a thousand cases |
| `retry.rs:341:9` | replace RetryingUpload::abort with Ok(()) | the call count in `abort_passes_through` (`retry/tests.rs`) |
| `upload.rs:49:24` | replace * with + (`CHUNK`) | the read count in `a_staged_file_larger_than_one_chunk_is_streamed_whole` (`upload/tests.rs`) |
| `upload.rs:253:23` | replace match guard newest > confirmed with true in list_step | `a_listing_does_not_read_back_a_tail_already_confirmed` (`upload/tests.rs`) |
| `created_at.rs:41:9` | replace ResolvedCreatedAt::len -> usize with 0 | `the_ledger_counts_each_created_at_resolved_through_the_store` (`created_at.rs`) |
| `created_at.rs:41:9` | replace ResolvedCreatedAt::len -> usize with 1 | same |
| `created_at.rs:47:9` | replace ResolvedCreatedAt::is_empty -> bool with false | same |
| `actor.rs:293:5` | replace leaves_bad_snapshot -> bool with true | `a_snapshot_that_fails_before_its_commit_deletes_nothing` (`actor/tests/rejected_snapshot.rs`) |
| `actor.rs:586:13` | replace && with \|\| in Actor::checkpoint_decision | `a_failed_checkpoint_waits_out_its_backoff` (`actor/tests/tasks.rs`, as are the rest) |
| `actor.rs:587:13` | replace && with \|\| in Actor::checkpoint_decision | same |
| `actor.rs:616:42` | replace && with \|\| in Actor::wal_unit | `an_idle_tick_does_not_open_the_wal` |
| `actor.rs:617:77` | delete ! in Actor::wal_unit | same |
| `actor.rs:630:16` | replace > with >= in Actor::wal_unit | `a_restarted_wal_with_nothing_new_is_not_reported_as_a_sync` |
| `actor.rs:757:42` | replace match guard !epoch_moved with true in Actor::absorb | `a_sync_whose_epoch_moved_does_not_checkpoint_and_asks_for_the_next_tick` |
| `actor.rs:821:33` | replace == with != in Actor::checkpoint_unit | `a_checkpoint_reports_busy_exactly_when_it_was` |
| `actor.rs:917:40` | replace && with \|\| in Actor::prune_unit | `a_failed_prune_waits_out_its_backoff` |
| `actor.rs:931:31` | replace == with != in Actor::prune_unit | `a_prune_continues_past_a_full_page` |
| `actor.rs:1170:17` | replace \|\| with && in Actor::ready_work | `a_pending_snapshot_asks_for_the_next_tick_at_once` |
| `actor.rs:1171:17` | replace \|\| with && in Actor::ready_work | same |
| `actor.rs:1172:17` | replace \|\| with && in Actor::ready_work | `a_sync_whose_epoch_moved_does_not_checkpoint_and_asks_for_the_next_tick` |
| `actor.rs:1213:48` | replace > with >= in Actor::next | `an_interval_due_with_nothing_to_checkpoint_does_not_spin` |

Two test doubles grew a knob for these: the actor tests' `Counting` store can cap a listing page (a store may
return fewer keys than the limit) and can move the WAL's read-lock hold when the next upload begins, as another
connection's checkpoint would in the middle of a sync step; it also logs the keys it is asked to delete.

#### Equivalent (9)

| Where | Mutation | Why |
| --- | --- | --- |
| `plan.rs:110:13` | `ContiguousChain::push`: `!snapshot && touches` becomes `\|\|` | Both callers push only a file that passes all three conjuncts: `extend` takes its `best` from the level 0-8 sweep, through `next_if(touches(.., chain.reaches()))`, and only `if info.key.range().max() > chain.reaches()`; `AsOfPlanner::extend` pushes the heap top of the same sweep, entered when it touched a reach no later than this one, after `if info.key.range().max() <= reach { break; }`. |
| `plan.rs:111:13` | same, `touches && max > reaches` becomes `\|\|` | Same callers. |
| `plan.rs:111:28` | same, `max > reaches` becomes `>=` | Same callers. |
| `plan.rs:363:70` | `straddles`: `max > one_past` becomes `>=` | Both callers (`straddling_file`, `lazy_straddler`) return first `if files.iter().any(\|info\| info.key.range().max() == one_past)`, so no file they pass in ends at `one_past`. |
| `plan.rs:396:66` | `extend`: `preference(&info) > preference(&held)` becomes `>=` | The preference (last transaction, level, first transaction) is the whole key, and an inventory holds each key once, so two different files never tie. |
| `plan.rs:432:45` | `explain_shortfall`: `max > requested` becomes `>=` | A usable file that touches the reach and ends at the requested transaction is eligible for that target, so the base or the sweep would have taken the chain to the target and there is no shortfall to explain. |
| `store.rs:141:49` | `build_listing`: `name > low` becomes `>=` | `low` is the cursor's canonical key name, and every store (`MemStore`, `store-fs`'s `classify`, `store-s3`'s list response) puts a name in `unrecognized` only when `parse_file_name` fails on it, which a canonical name never does. (`MemStore::plant_unrecognized` is documented as taking a name that is not a key.) |
| `layout.rs:266:30` | `FileKey::from_str`: `(raw << 4) \| digit` becomes `^` | The first run's entry in the table above: the operands never overlap. |
| `actor.rs:767:53` | `Actor::absorb`: `Observed::of(.., !self.more_wal)` loses the `!` | The `Observed` built there goes only to `after_sync`, which reads `synced_frames` (through `over_truncate`) and the count of transactions written, never `through`. |

#### Explained, not killed (1)

| Where | Mutation | Why |
| --- | --- | --- |
| `actor.rs:931:54` | `Actor::prune_unit`: `keys.len() == listing.files.len() && listing.next.is_some()` becomes `\|\|` | The two differ only when the page holds a key past the bound and another page follows. `through` is then the last prunable key's end instead of the bound, which costs one more staging listing on the next tick (it finds nothing left at or below the bound and records the bound); no file is deleted or kept differently. They cannot differ at all while each prune bound is the end of a staged level-0 file (the confirmed transaction, the level-1 end compacted from those files) or one below the newest staged file (the newest staged file is then the page's last key and no page follows). A bound that is not a staged end needs staging and the replica to disagree on key boundaries, a state no test builds; the cost if wrong is one listing. |

#### Harness (7)

The seven `conformance.rs` survivors are the ones the first run left, at the same lines. The suite's
self-tests now catch them; see "Kept test helpers" below.

#### Timeouts (13): the loop that spins

Each makes a loop stop ending; the tests that reach it hang and are killed, which is a failing test, not a
survivor. For the actor and maintenance mutants other tests also fail outright; the ones that hang are named.

| Where | Mutation | Loop that spins |
| --- | --- | --- |
| `actor.rs:397:27` | replace match guard now < prev with true in Actor::clock | After one reported clamp the clock answers the first instant forever, so the actor never sees time pass and `retention_failure`'s tick loop (actor tests) follows `Next::At` forever. Also fails `a_failed_prune_blocks_nothing_else`. |
| `actor.rs:397:31` | replace < with > in Actor::clock | Every forward step reads as a step back, the same frozen clock and the same loop. Also fails `a_failed_prune_blocks_nothing_else`. |
| `actor.rs:1173:61` | delete ! in Actor::ready_work | The upload task counts as ready when it is idle, so `next` is always `Now` and `retention_failure`'s loop follows it forever; `settle` panics as never settled in three other tests. |
| `compact.rs:371:9` | replace Reader::refill with Ok(true) | The end of an input is never reported, so `Reader::step` refills from an empty read forever. |
| `compact.rs:382:16` | replace > with >= in Reader::refill | An empty read counts as more input: the same loop. |
| `compact.rs:396:25` | replace >= with < in Reader::step | A consumed chunk is never refilled, so `Reader::step` loops on an empty remainder. |
| `maintain.rs:472:72` | replace > with == in plan_level | A source file that begins at the destination's last transaction counts as above it, so the level always has a window to compact again and `retention_failure`'s loop never reaches the retention delete. Also fails `a_crash_between_prunes_boots_clean`. |
| `maintain.rs:472:72` | replace > with >= in plan_level | Same. |
| `maintain.rs:555:5` | replace created_after with Ok(true) | The day's snapshot always counts as taken, so retention, which follows it, never runs and `retention_failure`'s loop follows each window's `Next::At` forever. Also fails `a_crash_between_prunes_boots_clean`. |
| `restore.rs:508:16` | replace < with == in apply_file | The read loop ends only on a chunk of exactly the read length; past the end every read is empty, forever (every actor test that restores). |
| `restore.rs:508:16` | replace < with > in apply_file | The loop never ends on a short chunk: the same. |
| `store.rs:80:9` | replace ReadLen::get with 0 | Every read returns nothing, and the uploader's read loops (`same_bytes` and the staged copy) end only on a chunk shorter than `ReadLen::get`, now 0. |
| `upload.rs:198:17` | replace && with \|\| in upload_tick | A tick with work returns at once with nothing put and `Next::Now`, so `files_per_tick_is_honoured_and_asks_to_step_again`'s loop ticks forever. Also fails `a_tick_that_fails_keeps_what_it_confirmed`. |

### Second run, the rest: survivors resolved

The second run finished: 586 mutants in 78 minutes, 349 caught, 131 unviable, 85 missed, 21 timeouts. The
first batch above is 45 of the missed and 13 of the timeouts; this section is the other 40 missed and 8
timeouts. Line numbers are those of commit `ec5bc07`, as above.

| File | Killed | Equivalent | Timeout |
| --- | --- | --- | --- |
| `verify.rs` | 8 | | 1 |
| `verify/dry_run.rs` | 1 | 2 | |
| `maintain/retain.rs` | 3 | 3 | 1 |
| `maintain/state.rs` | 3 | | |
| `actor/state.rs` | 5 | | |
| `upload.rs` | 2 | | 6 |
| Total | 22 | 5 | 8 |

The other 13 missed were in `source/fake.rs` (11) and `restore/mem_sink.rs` (2), which are now excluded
from mutation (see "Scope"), so they are not listed. No survivor revealed a bug.

Tally of the whole second run, both batches: of 85 missed, 50 are caught by new tests, 14 are equivalent,
1 is explained but not killed, 7 are the conformance-harness survivors (caught since, below) and 13 are in
the files now excluded. All 21 timeouts are explained. No survivor in `core` is unexplained.

#### Caught by new tests (22)

Each was run again with the targeted command (both test packages, `--file` and `--re` on its line and
column) and is caught.

| Where | Mutation | Test |
| --- | --- | --- |
| `verify.rs:71:28` | replace * with + (`CHUNK_BYTES`) | `a_file_longer_than_one_chunk_is_read_whole_in_four_mebibyte_reads` (`verify/tests.rs`, as are the next eight) |
| `verify.rs:71:35` | replace * with + (`CHUNK_BYTES`) | same |
| `verify.rs:71:35` | replace * with / (`CHUNK_BYTES`) | same |
| `verify.rs:757:20` | replace < with <= in Walk::read | same: a full chunk is not the end of the object |
| `verify.rs:310:9` | replace LevelSummary::bytes -> u64 with 1 | `level_summaries_count_files_bytes_and_range` now checks the sum |
| `verify.rs:322:9` | replace LevelSummary::skipped -> usize with 0 | `level_summaries_count_unrecognized_names` |
| `verify.rs:521:55` | replace && with \|\| in name_findings | `a_stray_level_zero_base_beside_a_newer_snapshot_reports_nothing` |
| `verify.rs:656:41` | replace == with != in Walk::finish | `the_page_size_reference_is_the_base_even_when_the_odd_file_is_read_first` |
| `verify/dry_run.rs:32:9` | replace DigestSink::truncate with Ok(()) | `a_database_that_shrinks_then_grows_verifies_clean` |
| `maintain/retain.rs:321:5` | replace aged -> bool with true | `a_last_file_without_created_at_holds_only_at_level_zero_or_nine` (`maintain/retain/tests.rs`) |
| `maintain/retain.rs:321:22` | replace == with != in aged | same |
| `maintain/retain.rs:470:57` | replace < with <= in level_zero | `a_level_zero_file_ending_at_the_floor_is_not_below_it` |
| `maintain/state.rs:136:9` | replace MaintainState::record_delete with () | `a_failed_delete_of_a_distrusted_output_backs_its_level_off` (`maintain/tests.rs`) |
| `maintain/state.rs:194:63` | replace == with != in MaintainState::retry_at | same |
| `maintain/state.rs:207:9` | replace MaintainState::forget_unlisted with () | `a_distrusted_output_gone_from_the_listing_is_forgotten` |
| `actor/state.rs:88:9` | replace Retry::expire with () | `a_passed_wait_with_no_work_left_is_not_a_deadline` (`actor/tests/tasks.rs`, as are the rest) |
| `actor/state.rs:88:55` | replace > with >= in Retry::expire | same |
| `actor/state.rs:197:32` | replace match guard at <= now with false in Gate::expire | same |
| `actor/state.rs:180:9` | replace Gate::open -> bool with true | `a_busy_truncate_waits_for_the_next_write` |
| `actor/state.rs:182:36` | replace >= with < in Gate::open | `a_busy_truncate_waits_one_interval` |
| `upload.rs:331:17` | replace < with <= in same_bytes | `a_listed_copy_that_differs_past_the_first_chunk_is_uploaded_again` (`upload/tests.rs`) |
| `upload.rs:389:17` | replace < with <= in decodes | `a_large_replica_tail_without_a_staged_copy_verifies_whole` |

#### Equivalent (5)

| Where | Mutation | Why |
| --- | --- | --- |
| `verify/dry_run.rs:37:9` | `DigestSink::reset` becomes `Ok(())` | Restore resets the sink only to apply a whole chain again from transaction 1, and that chain writes every page any earlier attempt wrote (at its newest version) or truncates it away, so nothing a reset would have cleared reaches the checksum. |
| `verify/dry_run.rs:118:9` | the dry run's `Counting::delete` becomes `Ok(())` | `restore_from` never deletes: restore, the planner, the inventory and the created-at time ledger only list and read. |
| `maintain/retain.rs:273:82` | `hole`: `max() > snapshot` becomes `>=` | A file that ends at the newest snapshot's end starts at or below it, so it never starts past `reach + 1`, and `reach.max(..)` leaves `reach`, which is already at least the snapshot's end, unchanged. |
| `maintain/retain.rs:287:12` | `hole`: `reach < newest` becomes `>` | `reach` only takes file ends, so it never passes `newest` and `>` is never true; and `<` is true only when the newest file is a level-9 file that does not start at 1, which `gate` holds as a malformed snapshot before it calls `hole`. |
| `maintain/retain.rs:537:37` | `self_check`: guard `Some(chain.reaches()) == inventory.newest()` becomes `true` | No rule deletes the last file of a level, so the newest file survives every job, and a `Latest` plan of the rest is either no plan or one that reaches the rest's newest, which is the listing's. |

#### Test doubles (20)

Seven were the conformance-harness survivors, caught since (see "Kept test helpers"). The other thirteen
were in `source/fake.rs` and `restore/mem_sink.rs`, which are no longer mutated: a mutant there changes what
a test asks of the engine, not what the engine does, and the fakes are checked against real SQLite by
`crates/compat/tests/compat/live_matches_sim.rs` (see "Scope").

#### Timeouts (8): the loop that spins

| Where | Mutation | Loop that spins |
| --- | --- | --- |
| `upload.rs:331:17` | replace < with == in same_bytes | The comparison ends only on a chunk of exactly the read length; past the end both reads are empty and equal, forever (every actor test that boots against a replica tail). |
| `upload.rs:331:17` | replace < with > in same_bytes | It never ends on a short chunk: the same. |
| `upload.rs:389:17` | replace < with == in decodes | The decode loop reads empty chunks past the end forever. |
| `upload.rs:389:17` | replace < with > in decodes | Same. |
| `upload.rs:592:17` | replace < with == in copy_verified | The staged copy reads empty chunks past the end forever. |
| `upload.rs:592:17` | replace < with > in copy_verified | Same. |
| `verify.rs:757:20` | replace < with > in Walk::read | The walk's read loop never ends on a short chunk and reads empty chunks forever (every `Depth::Contents` test). |
| `maintain/retain.rs:362:15` | replace <= with > in Found::wake_at | Retention only names wake instants past the newest created-at time, which the anchor never reaches, so `retention_failure`'s tick loop (actor tests) follows `Next::At` forever. Also fails `a_covered_level_zero_file_wakes_when_it_is_old_enough`. |

### Kept test helpers: `mem.rs` and `conformance.rs`

The suite's self-tests in `crates/core/src/conformance/tests.rs` run its cases against a wrapped `MemStore`
that breaks one promise and require the case to fail, with a message that names the check. They also pin the
harness behaviour the cases rely on.

| Test | Promise or behaviour | Kills (first-run line numbers) |
| --- | --- | --- |
| `a_store_that_reads_back_nothing_after_a_late_failed_commit_fails_the_suite` | after a commit that fails once its object is published, reads agree with the listing | `379:8` (`delete !`), `128:9` (returns false), `28:9` (empty message) |
| `a_store_that_lists_another_exact_created_at_fails_the_suite` | a listed created-at time is the committed one or marked last-modified | `634:5` (`exact_or_marked` true), `28:9` |
| `a_harness_that_cannot_inject_skips_the_late_commit_case` | the trait's default declines to inject, so a store that cannot be made to fail late is not failed for it | `99:9` (default true), `379:8` |
| `the_mem_harness_injects_a_late_commit_failure` | the in-memory harness does inject | `128:9` |
| `the_last_modified_harness_lists_last_modified` | the second in-memory harness really lists last-modified times, so the suite runs once in each listing mode | `111:9` |
| `block_on_resumes_a_future_that_was_woken` | `block_on` resumes a future that woke itself before it returned pending | `61:9`, by non-termination (below) |

Run: `just mutants core --file crates/core/src/conformance.rs --file crates/core/src/mem.rs`, `lsr-core`'s
own tests, at commit `dfc7886`. 73 mutants (49 in `conformance.rs`, 24 in `mem.rs`): 29 caught, 43 unviable,
1 timeout, 0 missed. No survivor in either file.

| File | Mutants | Caught | Unviable | Timeout | Missed |
| --- | --- | --- | --- | --- | --- |
| `conformance.rs` | 49 | 15 | 33 | 1 | 0 |
| `mem.rs` | 24 | 14 | 10 | 0 | 0 |

The one timeout is `conformance.rs:65:9`, `ThreadWaker::wake` replaced by `()`: the woken future is never
polled again because `block_on` stays parked, so `block_on_resumes_a_future_that_was_woken` hangs and is
killed. That is a failing test, not a survivor; no test can see a thread that never unparks other than by
waiting for it. Most of the unviable mutants replace a function returning `Result<(), Failure>`, a key, a
created-at time or a listing with `Default::default()`, which those types do not implement.

The four-job run filled the disk after 70 mutants and reported four of them unviable from the failed build
(`conformance.rs:65:9`, `71:5`, `504:12`, `mem.rs:103:9` with 0). They were run again at one job, with the
three it had not reached, and the tally above counts the second result.
