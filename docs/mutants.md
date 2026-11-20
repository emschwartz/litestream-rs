# Mutation testing

`just mutants <crate>` runs cargo-mutants 27.1.0 over `lsr-<crate>` with nextest, four jobs, the tree copied with
its `.git`, results in `target/mutants/mutants.out/`. No exclude patterns are configured. Phase 2's exit criterion is that every
survivor in `ltx` and `core` is killed by a test or listed here with a reason. `ltx` meets that; `core` is part way
(see its section).

## ltx

Every mutant not listed below is caught or unviable (does not compile). Tests added for the first run's
survivors: `tests/ltx/pins.rs`, the `tests` modules of `checksum.rs`, `encoder.rs`, `frame.rs`, `ids.rs`
and `page.rs`.

Tally (581 generated, about 7 minutes at four jobs): first run 29 missed, 477 caught, 68 unviable, 7 timeouts;
final run 3 missed (all equivalent, below), 503 caught, 68 unviable, 7 timeouts (all below).

### Equivalent (no input can tell the mutant from the original)

| Where | Mutation | Why |
| --- | --- | --- |
| `checksum.rs` `Checksum::toggle` | `FLAG \| (a ^ b)` becomes `FLAG ^ (a ^ b)` | Both operands carry bit 63 (the type only holds flagged values), so `a ^ b` has it clear and OR and XOR of the flag agree. |
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

**Status: not finished.** The first run was stopped at 899 of 1463 mutants (about eight hours on four shared
cores), and the survivors of the files it finished are only partly triaged. The exit bar (zero unexplained
survivors) is not met; what is open is listed below so the next run starts from it instead of from nothing.

The recipe needed one change before it could run at all: `core` has a guard test that lists the tracked files
with git, cargo-mutants copies the tree without `.git` by default, and so the unmutated baseline failed and no
mutant was tested. `just mutants` now passes `--copy-vcs true`.

Tally of the first run so far: 491 caught, 88 missed, 13 timeouts, 307 unviable. Of the 88 missed,
31 are now caught by new tests (each run again and confirmed caught), 1 is equivalent, 7 are in the store
conformance harness, and 49 are open.

This run executes `lsr-core`'s own tests only. The actor's widest tests are the simulator's scenarios in
`lsr-sim`, which a `-p lsr-core` run does not execute. A trial with `--test-package lsr-core --test-package lsr-sim`
reached two of the actor's survivors before it was stopped for time: it caught `Actor::request_snapshot` (since
also caught by a core test) and missed `actor.rs:573:13`. The open actor survivors below should be run that way
before a test is written for each.

### Caught by new tests (31)

Tests added: `entropy_is_splitmix64` and `below_scales_the_whole_output` (`env.rs`),
`the_longest_backoff_is_five_minutes` (`fault/tests.rs`),
`stamped_keeps_every_file_and_gives_each_its_stored_stamp` (`inventory.rs`),
`debug_and_the_range_side_print_what_a_log_line_needs` (`layout.rs`), `the_chunk_is_one_mebibyte`,
`a_small_output_is_written_to_the_upload_once` and a lower bound on the measured peak in the memory-bound test
(`compact/tests.rs`), `the_failed_state_stays_at_the_threshold`,
`a_position_after_a_checkpoint_is_pinned_at_the_epoch_it_retook`,
`the_seal_takes_every_committed_transaction_whatever_the_cap` and the pin assertion in
`a_passive_held_back_by_a_reader_is_not_restarted` (`checkpoint/tests.rs`), `a_first_boot_lists_each_level_once`
and `a_requested_snapshot_is_staged_on_a_quiet_wal` (`actor/tests.rs`).

| Where | Mutation |
| --- | --- |
| `actor.rs:335:9` | replace Actor::request_snapshot with () |
| `actor.rs:1190:52` | replace match guard confirmed.is_some() with true in boot_recover |
| `checkpoint.rs:218:62` | replace match guard !over_truncate(observed, cfg) with true in after_sync |
| `checkpoint.rs:386:9` | replace Protocol<'_, W, P, C, L>::pin with () |
| `checkpoint.rs:386:29` | replace == with != in Protocol<'_, W, P, C, L>::pin |
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
| `inventory.rs:112:9` | replace Inventory::stamped -> Result<Self, StoreError> with Ok(Default::default()) |
| `layout.rs:93:9` | replace <impl fmt::Display for RangeSide>::fmt -> fmt::Result with Ok(Default::default()) |
| `layout.rs:245:9` | replace <impl fmt::Debug for FileKey>::fmt -> fmt::Result with Ok(Default::default()) |

### Equivalent (no input can tell the mutant from the original)

| Where | Mutation | Why |
| --- | --- | --- |
| `layout.rs` `FileKey::from_str` | `(raw << 4) \| digit` becomes `^` | The shift leaves the low nibble zero and `digit` is below 16, so the operands never overlap. |

### In the store conformance harness (explained, not killed)

`conformance.rs` is the suite every `ObjectStore` runs, built only for tests. A mutant here does not change the
engine: it makes the suite ask less of a store, and the stores in this repository are correct, so they pass the
weaker suite too. Nothing runs the suite against a store that is wrong on purpose; that self-test is what would
kill these, and it is not written.

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
That is a failing test, not a survivor, but the loop each one spins has not been written down one by one as the
`ltx` section does; until it is, treat them as unconfirmed.

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
| `maintain.rs:535:5` | replace stamped_after -> Result<bool, FileInfo> with Ok(true) |
| `restore.rs:508:16` | replace < with == in apply_file |
| `restore.rs:508:16` | replace < with > in apply_file |
| `retry.rs:205:9` | replace Core<S, P, E>::wait_after -> Option<Duration> with Some(Default::default()) |
| `store.rs:80:9` | replace ReadLen::get -> u64 with 0 |

### Open: survivors not yet killed or explained (49)

| Where | Mutation |
| --- | --- |
| `actor.rs:573:13` | replace && with \|\| in Actor::checkpoint_decision |
| `actor.rs:572:13` | replace && with \|\| in Actor::checkpoint_decision |
| `actor.rs:602:42` | replace && with \|\| in Actor::wal_unit |
| `actor.rs:603:77` | delete ! in Actor::wal_unit |
| `actor.rs:604:21` | delete ! in Actor::wal_unit |
| `actor.rs:616:16` | replace > with >= in Actor::wal_unit |
| `actor.rs:743:42` | replace match guard !epoch_moved with true in Actor::absorb |
| `actor.rs:753:53` | delete ! in Actor::absorb |
| `actor.rs:807:33` | replace == with != in Actor::checkpoint_unit |
| `actor.rs:903:40` | replace && with \|\| in Actor::prune_unit |
| `actor.rs:917:31` | replace == with != in Actor::prune_unit |
| `actor.rs:917:54` | replace && with \|\| in Actor::prune_unit |
| `actor.rs:1115:17` | replace \|\| with && in Actor::ready_work |
| `actor.rs:1114:17` | replace \|\| with && in Actor::ready_work |
| `actor.rs:1113:17` | replace \|\| with && in Actor::ready_work |
| `actor.rs:1156:41` | replace && with \|\| in Actor::next |
| `actor.rs:1156:48` | replace > with == in Actor::next |
| `actor.rs:1156:48` | replace > with < in Actor::next |
| `actor.rs:1156:48` | replace > with >= in Actor::next |
| `plan.rs:111:13` | replace && with \|\| in ContiguousChain::push |
| `plan.rs:110:13` | replace && with \|\| in ContiguousChain::push |
| `plan.rs:111:28` | replace > with >= in ContiguousChain::push |
| `plan.rs:363:70` | replace > with >= in straddles |
| `plan.rs:396:66` | replace > with >= in extend |
| `plan.rs:397:39` | replace > with >= in extend |
| `plan.rs:441:41` | replace && with \|\| in explain_shortfall |
| `plan.rs:432:45` | replace > with >= in explain_shortfall |
| `plan.rs:563:9` | replace AsOfPlanner<'a>::comparisons -> u64 with 1 |
| `plan.rs:563:9` | replace AsOfPlanner<'a>::comparisons -> u64 with 0 |
| `plan.rs:712:65` | replace < with <= in lazy_straddler |
| `plan.rs:733:5` | replace depth -> u64 with 0 |
| `plan.rs:733:5` | replace depth -> u64 with 1 |
| `recover.rs:180:50` | replace > with >= in TipView::load |
| `recover.rs:349:61` | replace match guard end.frame_index() == 0 with true in match_last_page |
| `recover.rs:661:22` | replace < with <= in restarted |
| `recover.rs:761:31` | replace match guard at_start with false in frame_state |
| `recover.rs:756:42` | replace match guard at_start with true in frame_state |
| `recover.rs:756:42` | replace match guard at_start with false in frame_state |
| `restore.rs:75:9` | replace <impl std::fmt::Display for SinkOp>::fmt -> std::fmt::Result with Ok(Default::default()) |
| `retry.rs:341:9` | replace <impl Upload for RetryingUpload<S, P, E>>::abort -> Result<(), StoreError> with Ok(()) |
| `source.rs:188:9` | replace Barrier::id -> u64 with 0 |
| `source.rs:188:9` | replace Barrier::id -> u64 with 1 |
| `source.rs:206:9` | replace WriteLock::id -> u64 with 0 |
| `source.rs:206:9` | replace WriteLock::id -> u64 with 1 |
| `stamps.rs:41:9` | replace Stamps::len -> usize with 0 |
| `stamps.rs:41:9` | replace Stamps::len -> usize with 1 |
| `stamps.rs:47:9` | replace Stamps::is_empty -> bool with true |
| `stamps.rs:47:9` | replace Stamps::is_empty -> bool with false |
| `store.rs:141:49` | replace > with >= in build_listing |

### Not yet run

The run goes file by file. These files had mutants left when it was stopped:

| File | Mutants not yet tested |
| --- | --- |
| `actor/state.rs` | 34 |
| `maintain/config.rs` | 19 |
| `maintain/retain.rs` | 126 |
| `maintain/state.rs` | 18 |
| `restore/mem_sink.rs` | 34 |
| `source/fake.rs` | 102 |
| `store.rs` | 1 |
| `sync.rs` | 41 |
| `upload.rs` | 79 |
| `verify.rs` | 92 |
| `verify/dry_run.rs` | 18 |

To continue: `just mutants core --file <path>` for each file above (`--file` may be given more than once), then
triage as above. `source/fake.rs` and `mem.rs` are test doubles like the conformance harness; decide once whether
such files are mutated at all, and say so here.
