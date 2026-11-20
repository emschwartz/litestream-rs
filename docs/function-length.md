# Function length checkpoint (decision D5)

Phase 1's exit asks: list every function with exactly one caller, and if many exist only to satisfy the 70-line
lint, raise the limit. Taken at `b901dff`.

**Result: the limit is not creating them. Recommendation: keep 70.** The decision stays the owner's (D5).

## Numbers

| | |
| --- | --- |
| Functions outside test code | 1779 |
| Function names defined more than once (not judged, see Method) | 202 |
| Functions with exactly one call site | 375 |
| of which public | 158 |
| of which private | 217 |
| Private, and folding it into its caller would pass 70 lines | 31 |
| Private, and folding it into its caller would still fit | 186 |
| Functions of 61 to 70 lines | 4 |
| Functions the count puts above 70 lines | 4 |
| `#[expect(clippy::too_many_lines)]` in the workspace | 1 (`finding_parts` in `cli/src/render.rs`: one match arm per finding) |

Private single-caller functions by crate (and how many of them would pass the limit if folded): `core` 87 (18),
`store-s3` 27 (0), `compat` 26 (5), `sim` 24 (5), `ltx` 18 (0), `cli` 17 (1), `fuzzing` 6 (0), `wal` 5 (2),
`store-fs` 4 (0), `local` 3 (0). By their own length: 15 are 5 lines or fewer, 90 are 6 to 15, 78 are 16 to 30 and
34 are longer.

## Reading

- **186 of the 217 are a choice, not a consequence.** Folded back into its only caller, each would still fit under
  the limit. They exist to give a step a name: `abort_quietly`, `append_bounded`, `base_headers`, `aged`. The limit
  did not ask for them.
- **The 31 that would pass the limit are named units, not second halves.** Their names were read for all 31 and
  the bodies for 14 (`boot_recover`, `checkpoint_unit`, `maintain_unit`, `run_job`, `plan_level`, `plan_snapshot`,
  `agrees`, `newest_level_zero`, `scan_pages`, `tip_range`, `run_one`, `note_snapshots`, `note_tick`, `follow`).
  Each is one lane of the actor, one level's plan, one rule of recovery or one piece of the simulator's
  bookkeeping, with its own doc comment and its own inputs. None is a function cut where the line count ran out
  (no `part_two`, no helper taking a caller's ten locals). The clearest case for the limit shaping the code is
  `TipView::load` in `core/src/recover.rs`, whose five single-caller helpers add up to 149 lines; as one function
  it would be over 200 lines, and the helpers are the steps it takes (find the newest staged file, re-derive the
  tip's range from the WAL, decode the staged file, match its last page, compare the two).
- **Little presses against the limit.** Four functions sit within ten lines of it and one carries an `expect`. A
  limit that forced many splits would show a crowd just under 70.
- **No commit says a function was split to satisfy the lint** (`git log --grep` for the lint's name, "70 line",
  "line limit" and "function length" finds nothing).
- More than half of the 158 public ones are the simulator's (87): its scenarios, each named once in a table (47),
  and helpers its runner calls once. The rest are API with one caller inside the workspace so far. A public
  function with one caller says nothing about the limit.

What would change the recommendation: a later checkpoint that finds helpers named after their position rather than
their job, or more than a handful of `expect`s.

## Method and its limits

`scripts/single-caller.py` produced the numbers and the tables (`--markdown` for the tables). It reads
`crates/*/src`, skips test code (`tests.rs`, `tests/` directories, `#[cfg(test)] mod` blocks) and matches names,
not types:

- A function is judged only when its name is defined once in the workspace. The 202 names defined more than once
  (`new`, `get`, `list`, `read` and the like) are counted and left out, so the list is complete for unique names
  only. Those names are mostly constructors, accessors and trait methods, which have many callers.
- A call site is the name followed by `(` or `::<`, or passed as a value. Calls through macros, trait dispatch or
  a re-export under another name are not followed. `use` lines are not call sites.
- Lengths count lines that hold code from the `fn` line to the closing brace: the signature is included, blank
  and comment-only lines are not. Clippy's `too_many_lines` counts the body only, so this count runs a few lines
  high; that is why it puts four functions above 70 that clippy accepts (`run` in `core/src/sync.rs`, `load` in
  `core/src/recover.rs`, `attempt` in `compat/src/generate.rs`, and the one with the `expect`).

Regenerate with `python3 scripts/single-caller.py --markdown`.

## Private functions with one caller that would pass 70 lines if folded (31)

| File | Function | Lines | Only caller | Caller lines | Sum | Above the limit if folded |
| --- | --- | --- | --- | --- | --- | --- |
| `core/src/recover.rs` | `agrees` | 9 | `load` | 72 | 81 | yes |
| `core/src/restore.rs` | `attempt_once` | 29 | `restore_from` | 48 | 77 | yes |
| `core/src/actor.rs` | `boot_recover` | 28 | `boot` | 47 | 75 | yes |
| `wal/src/scan.rs` | `check_cursor` | 36 | `next_tx` | 53 | 89 | yes |
| `core/src/actor.rs` | `checkpoint_unit` | 51 | `wal_unit` | 35 | 86 | yes |
| `sim/src/run.rs` | `follow` | 11 | `step` | 62 | 73 | yes |
| `compat/src/generate.rs` | `gap_targets` | 46 | `record_targets` | 26 | 72 | yes |
| `core/src/maintain/retain.rs` | `level_zero` | 34 | `candidates` | 49 | 83 | yes |
| `core/src/actor.rs` | `maintain_unit` | 53 | `run` | 19 | 72 | yes |
| `core/src/recover.rs` | `match_last_page` | 24 | `load` | 72 | 96 | yes |
| `core/src/verify.rs` | `name_findings` | 51 | `verify` | 31 | 82 | yes |
| `core/src/recover.rs` | `newest_level_zero` | 14 | `load` | 72 | 86 | yes |
| `sim/src/run.rs` | `note_snapshots` | 47 | `step` | 62 | 109 | yes |
| `sim/src/run.rs` | `note_tick` | 16 | `step` | 62 | 78 | yes |
| `core/src/verify.rs` | `older_history` | 23 | `name_findings` | 51 | 74 | yes |
| `wal/src/scan.rs` | `partial_frame` | 19 | `next_tx` | 53 | 72 | yes |
| `core/src/maintain.rs` | `plan_level` | 51 | `decide` | 43 | 94 | yes |
| `core/src/maintain.rs` | `plan_snapshot` | 34 | `decide` | 43 | 77 | yes |
| `compat/src/generate.rs` | `publish_replica` | 42 | `attempt` | 71 | 113 | yes |
| `compat/src/generate.rs` | `record_targets` | 26 | `attempt` | 71 | 97 | yes |
| `compat/src/generate.rs` | `restorable_targets` | 46 | `record_targets` | 26 | 72 | yes |
| `core/src/actor.rs` | `run_job` | 31 | `maintain_unit` | 53 | 84 | yes |
| `sim/src/run.rs` | `run_one` | 20 | `step` | 62 | 82 | yes |
| `core/src/recover.rs` | `same_generation` | 40 | `recover_traced` | 44 | 84 | yes |
| `core/src/recover.rs` | `scan_pages` | 49 | `load` | 72 | 121 | yes |
| `core/src/maintain/retain.rs` | `self_check` | 30 | `decide` | 44 | 74 | yes |
| `sim/src/scenario/faults.rs` | `throttle_waits` | 55 | `store_429_retry_after` | 25 | 80 | yes |
| `core/src/recover.rs` | `tip_range` | 53 | `load` | 72 | 125 | yes |
| `cli/src/render.rs` | `verify_text` | 47 | `done` | 31 | 78 | yes |
| `compat/src/generate.rs` | `wait_for_new_snapshot` | 22 | `attempt` | 71 | 93 | yes |
| `core/src/compact.rs` | `write_output` | 33 | `compact_with` | 41 | 74 | yes |

## Private functions with one caller that would still fit if folded (186)

| File | Function | Lines | Only caller | Caller lines | Sum | Above the limit if folded |
| --- | --- | --- | --- | --- | --- | --- |
| `core/src/compact.rs` | `abort_quietly` | 5 | `compact_with` | 41 | 46 | no |
| `core/src/retry.rs` | `add_wait` | 7 | `run` | 21 | 28 | no |
| `core/src/maintain/retain.rs` | `aged` | 3 | `unstamped` | 11 | 14 | no |
| `core/src/compact.rs` | `all_mut` | 3 | `next_page` | 23 | 26 | no |
| `core/src/maintain/retain.rs` | `anchor_files` | 14 | `decide` | 44 | 58 | no |
| `sim/src/run.rs` | `app_lets_go` | 8 | `idle` | 44 | 52 | no |
| `store-s3/src/upload.rs` | `append_bounded` | 11 | `append` | 22 | 33 | no |
| `core/src/restore.rs` | `apply_chain` | 20 | `attempt_once` | 29 | 49 | no |
| `core/src/plan.rs` | `as_of_outcome` | 14 | `plan_exact` | 38 | 52 | no |
| `wal/src/scan.rs` | `bad_frame` | 11 | `next_tx` | 53 | 64 | no |
| `store-s3/src/request.rs` | `base_headers` | 7 | `sign` | 15 | 22 | no |
| `store-fs/src/lib.rs` | `begin_now` | 20 | `begin_upload` | 7 | 27 | no |
| `ltx/src/frame.rs` | `block_checksum_len` | 7 | `need` | 9 | 16 | no |
| `ltx/src/frame.rs` | `block_size` | 7 | `consume` | 20 | 27 | no |
| `core/src/checkpoint.rs` | `boundary_snapshot` | 12 | `truncate` | 23 | 35 | no |
| `compat/src/generate.rs` | `caught_up` | 17 | `settle_replica` | 13 | 30 | no |
| `compat/src/manifest.rs` | `check_files` | 24 | `check` | 15 | 39 | no |
| `fuzzing/src/index.rs` | `check_frame` | 23 | `index` | 23 | 46 | no |
| `fuzzing/src/index.rs` | `check_index` | 20 | `index` | 23 | 43 | no |
| `fuzzing/src/plan.rs` | `check_inside_file` | 28 | `check_reason` | 41 | 69 | no |
| `fuzzing/src/plan.rs` | `check_lazy` | 10 | `plan` | 11 | 21 | no |
| `core/src/maintain/config.rs` | `check_order` | 32 | `new` | 36 | 68 | no |
| `ltx/src/index.rs` | `check_page` | 10 | `check_entry` | 13 | 23 | no |
| `fuzzing/src/plan.rs` | `check_reason` | 41 | `plan` | 11 | 52 | no |
| `store-s3/src/fake.rs` | `check_signed_headers` | 19 | `handle` | 25 | 44 | no |
| `core/src/compact.rs` | `check_survives_shrink` | 12 | `write_output` | 33 | 45 | no |
| `compat/src/manifest.rs` | `check_targets` | 28 | `check` | 15 | 43 | no |
| `core/src/source/fake.rs` | `checkpoint_run` | 39 | `checkpoint` | 5 | 44 | no |
| `wal/src/scan.rs` | `checksum_before` | 17 | `verified_before` | 31 | 48 | no |
| `ltx/src/ids.rs` | `civil_from_days` | 12 | `fmt` | 21 | 33 | no |
| `cli/src/main.rs` | `command_line` | 11 | `execute` | 5 | 16 | no |
| `core/src/upload.rs` | `commit_failed` | 7 | `put_staged` | 24 | 31 | no |
| `store-fs/src/lib.rs` | `commit_now` | 36 | `commit` | 3 | 39 | no |
| `core/src/actor.rs` | `compact_fault` | 8 | `maintain_fault` | 8 | 16 | no |
| `core/src/compact.rs` | `compact_with` | 41 | `compact` | 8 | 49 | no |
| `store-s3/src/store.rs` | `content_range_start` | 9 | `range_body` | 30 | 39 | no |
| `core/src/verify.rs` | `contents_pass` | 28 | `verify` | 31 | 59 | no |
| `core/src/maintain.rs` | `contiguous_run` | 15 | `plan_level` | 51 | 66 | no |
| `core/src/recover.rs` | `continued` | 24 | `same_generation` | 40 | 64 | no |
| `core/src/upload.rs` | `copy_verified` | 40 | `put_staged` | 24 | 64 | no |
| `sim/src/run.rs` | `crash_tick` | 15 | `apply` | 47 | 62 | no |
| `ltx/src/ids.rs` | `days_from_civil` | 9 | `from_parts` | 18 | 27 | no |
| `ltx/src/ids.rs` | `days_in_month` | 8 | `parse_rfc3339` | 40 | 48 | no |
| `core/src/actor.rs` | `deadlines` | 19 | `next` | 12 | 31 | no |
| `ltx/src/index.rs` | `decode_frame_into` | 33 | `decode_frame` | 12 | 45 | no |
| `store-s3/src/fake.rs` | `decode_token` | 8 | `list` | 42 | 50 | no |
| `compat/src/workload.rs` | `delete_items` | 12 | `apply` | 11 | 23 | no |
| `store-fs/src/lib.rs` | `delete_now` | 10 | `delete` | 3 | 13 | no |
| `sim/src/scenario/faults.rs` | `deleted_then_put_again` | 38 | `etag_mismatch_on_put` | 13 | 51 | no |
| `cli/src/render.rs` | `depth_name` | 6 | `verify_json` | 37 | 43 | no |
| `ltx/src/frame.rs` | `descriptor_tail` | 10 | `consume` | 20 | 30 | no |
| `store-s3/src/fake.rs` | `entity_text` | 18 | `xml_unescape` | 19 | 37 | no |
| `compat/src/binaries.rs` | `era_of` | 10 | `new` | 6 | 16 | no |
| `store-s3/src/list.rs` | `fetch_page` | 30 | `next_listed` | 11 | 41 | no |
| `cli/src/render.rs` | `file_json` | 14 | `files_json` | 13 | 27 | no |
| `cli/src/run.rs` | `files_command` | 10 | `run` | 9 | 19 | no |
| `cli/src/render.rs` | `files_json` | 13 | `done` | 31 | 44 | no |
| `cli/src/run.rs` | `files_on_s3` | 23 | `files_command` | 10 | 33 | no |
| `cli/src/render.rs` | `files_text` | 15 | `done` | 31 | 46 | no |
| `compat/src/workload.rs` | `fill_scratch` | 10 | `apply` | 11 | 21 | no |
| `core/src/plan.rs` | `find_base` | 18 | `advance` | 13 | 31 | no |
| `compat/src/generate.rs` | `find_hole` | 21 | `publish_replica` | 42 | 63 | no |
| `cli/src/render.rs` | `finding_json` | 8 | `verify_json` | 37 | 45 | no |
| `store-s3/src/upload.rs` | `finish_multipart` | 30 | `commit` | 8 | 38 | no |
| `local/src/lib.rs` | `first_present` | 8 | `publish` | 12 | 20 | no |
| `core/src/compact.rs` | `first_unrewritten` | 7 | `check_unheld` | 13 | 20 | no |
| `core/src/maintain/retain.rs` | `foreign_names` | 9 | `gate` | 13 | 22 | no |
| `ltx/src/ids.rs` | `fraction_millis` | 23 | `parse_rfc3339` | 40 | 63 | no |
| `core/src/recover.rs` | `frame_state` | 24 | `same_generation` | 40 | 64 | no |
| `ltx/src/decoder.rs` | `frame_unit` | 34 | `step` | 22 | 56 | no |
| `fuzzing/src/plan.rs` | `furthest_reach` | 27 | `check_chain` | 39 | 66 | no |
| `store-s3/src/store.rs` | `head_stamp` | 12 | `stamp` | 5 | 17 | no |
| `compat/src/pages.rs` | `header_page_size` | 5 | `hash_pages` | 30 | 35 | no |
| `store-s3/src/config.rs` | `host_is_address_or_local` | 7 | `new` | 25 | 32 | no |
| `store-s3/src/fake.rs` | `http_date` | 24 | `object_headers` | 14 | 38 | no |
| `ltx/src/decoder.rs` | `index_byte` | 35 | `step` | 22 | 57 | no |
| `compat/src/workload.rs` | `insert_blob` | 10 | `apply` | 11 | 21 | no |
| `compat/src/workload.rs` | `insert_items` | 21 | `apply` | 11 | 32 | no |
| `store-s3/src/classify.rs` | `is_auth` | 10 | `classify` | 44 | 54 | no |
| `sim/src/world.rs` | `is_full_image` | 17 | `staged_is_full_image` | 4 | 21 | no |
| `cli/src/args.rs` | `is_help` | 3 | `scan` | 35 | 38 | no |
| `ltx/src/ids.rs` | `is_leap` | 3 | `days_in_month` | 8 | 11 | no |
| `store-s3/src/fake.rs` | `join_parts` | 26 | `complete` | 28 | 54 | no |
| `core/src/plan.rs` | `judge_past` | 26 | `advance` | 13 | 39 | no |
| `core/src/maintain/retain.rs` | `latest_unplannable` | 17 | `gate` | 13 | 30 | no |
| `core/src/plan.rs` | `lazy_straddler` | 27 | `judge_past` | 26 | 53 | no |
| `core/src/maintain.rs` | `leaves_bad_output` | 17 | `run_compact` | 34 | 51 | no |
| `ltx/src/frame.rs` | `legacy_block` | 22 | `consume` | 20 | 42 | no |
| `ltx/src/frame.rs` | `legacy_block_size` | 19 | `consume` | 20 | 39 | no |
| `cli/src/render.rs` | `level_json` | 11 | `verify_json` | 37 | 48 | no |
| `core/src/compact.rs` | `level_not_below` | 6 | `compact_with` | 41 | 47 | no |
| `core/src/maintain/retain.rs` | `levels_one_to_eight` | 20 | `candidates` | 49 | 69 | no |
| `local/src/lib.rs` | `link_and_sync` | 6 | `publish` | 12 | 18 | no |
| `compat/src/manifest.rs` | `list_replica_files` | 7 | `check` | 15 | 22 | no |
| `core/src/upload.rs` | `list_step` | 35 | `upload_next` | 15 | 50 | no |
| `sim/src/world.rs` | `listed_at` | 5 | `write_staged` | 7 | 12 | no |
| `core/src/maintain/retain.rs` | `listing_behind` | 5 | `gate` | 13 | 18 | no |
| `sim/src/resources.rs` | `lists_allowed` | 11 | `add` | 10 | 21 | no |
| `core/src/actor.rs` | `lose_staged` | 22 | `upload_unit` | 46 | 68 | no |
| `core/src/actor.rs` | `maintain_fault` | 8 | `run_job` | 31 | 39 | no |
| `sim/src/scenario.rs` | `matches` | 25 | `run_scenario` | 23 | 48 | no |
| `core/src/maintain/retain.rs` | `merged_ranges` | 17 | `level_zero` | 34 | 51 | no |
| `store-s3/src/ureq_transport.rs` | `method_name` | 9 | `build` | 13 | 22 | no |
| `core/src/actor.rs` | `mode_of` | 7 | `checkpoint_decision` | 12 | 19 | no |
| `store-s3/src/list.rs` | `name_bounds` | 11 | `fetch_page` | 30 | 41 | no |
| `core/src/verify/dry_run.rs` | `named_file` | 12 | `latest_restore` | 29 | 41 | no |
| `cli/src/args.rs` | `names_a_provider_endpoint` | 11 | `s3` | 32 | 43 | no |
| `store-s3/src/list.rs` | `next_listed` | 11 | `collect` | 14 | 25 | no |
| `core/src/compact.rs` | `next_page` | 23 | `write_output` | 33 | 56 | no |
| `store-s3/src/classify.rs` | `not_found` | 7 | `classify` | 44 | 51 | no |
| `sim/src/sqlite.rs` | `on_read` | 23 | `read` | 18 | 41 | no |
| `sim/src/scenario/faults.rs` | `one_clamp_no_fault` | 18 | `clock_steps_back_10s` | 14 | 32 | no |
| `store-fs/src/lib.rs` | `open_scratch` | 20 | `begin_now` | 20 | 40 | no |
| `core/src/compact.rs` | `output_header` | 21 | `compact_with` | 41 | 62 | no |
| `core/src/sync.rs` | `page_fault` | 9 | `fill` | 36 | 45 | no |
| `local/src/lib.rs` | `page_offset` | 6 | `write_page` | 5 | 11 | no |
| `core/src/restore/mem_sink.rs` | `page_size_of` | 6 | `write_page` | 15 | 21 | no |
| `compat/src/bin/compat-regen.rs` | `parse_args` | 22 | `main` | 34 | 56 | no |
| `sim/src/store.rs` | `parse_header` | 4 | `commit` | 22 | 26 | no |
| `store-s3/src/fake.rs` | `parse_parts` | 14 | `complete` | 28 | 42 | no |
| `store-s3/src/fake.rs` | `parse_range` | 9 | `serve_range` | 20 | 29 | no |
| `store-s3/src/upload.rs` | `part_len` | 3 | `append` | 22 | 25 | no |
| `core/src/verify.rs` | `plan_previous` | 4 | `check_chain` | 29 | 33 | no |
| `core/src/actor.rs` | `plan_step` | 21 | `step` | 33 | 54 | no |
| `core/src/restore.rs` | `plan_target` | 26 | `attempt_once` | 29 | 55 | no |
| `sim/src/sqlite.rs` | `power_crash` | 15 | `crash` | 6 | 21 | no |
| `core/src/actor.rs` | `prune_unit` | 46 | `run` | 19 | 65 | no |
| `core/src/upload.rs` | `put_staged` | 24 | `put_step` | 24 | 48 | no |
| `core/src/upload.rs` | `put_step` | 24 | `upload_next` | 15 | 39 | no |
| `sim/src/oracle.rs` | `range_str` | 3 | `covers` | 64 | 67 | no |
| `sim/src/world.rs` | `re_added` | 17 | `app_commit` | 13 | 30 | no |
| `wal/src/scan.rs` | `read_page_inner` | 27 | `read_page` | 7 | 34 | no |
| `ltx/src/decoder.rs` | `read_retrying` | 8 | `next_event` | 18 | 26 | no |
| `core/src/actor.rs` | `ready_work` | 18 | `next` | 12 | 30 | no |
| `store-s3/src/fault.rs` | `record_and_take` | 13 | `send` | 36 | 49 | no |
| `core/src/maintain/state.rs` | `record_compact` | 17 | `record` | 7 | 24 | no |
| `core/src/maintain/state.rs` | `record_delete` | 20 | `record` | 7 | 27 | no |
| `compat/src/rsrestore.rs` | `reencode_file` | 23 | `reencode` | 10 | 33 | no |
| `core/src/compact.rs` | `refill` | 19 | `step` | 35 | 54 | no |
| `sim/src/sqlite.rs` | `regrow` | 23 | `regrow_with` | 19 | 42 | no |
| `core/src/maintain.rs` | `remove_rejected` | 9 | `run_compact` | 34 | 43 | no |
| `store-s3/src/store.rs` | `request_error` | 7 | `request` | 7 | 14 | no |
| `cli/src/run.rs` | `restore_command` | 12 | `run` | 9 | 21 | no |
| `cli/src/render.rs` | `restored_text` | 25 | `done` | 31 | 56 | no |
| `cli/src/args.rs` | `s3` | 32 | `replica` | 15 | 47 | no |
| `core/src/upload.rs` | `same_bytes` | 26 | `tail_is_sound` | 31 | 57 | no |
| `core/src/sync.rs` | `scan_fault` | 16 | `scan` | 38 | 54 | no |
| `core/src/checkpoint.rs` | `seal_and_checkpoint` | 6 | `passive` | 19 | 25 | no |
| `store-s3/src/fake.rs` | `serve_range` | 20 | `get` | 13 | 33 | no |
| `compat/src/pages.rs` | `sha256_hex` | 10 | `hash_pages` | 30 | 40 | no |
| `compat/src/generate.rs` | `shape_ok` | 5 | `tail` | 18 | 23 | no |
| `core/src/actor.rs` | `snapshot_after` | 6 | `plan_step` | 21 | 27 | no |
| `core/src/actor.rs` | `snapshot_job` | 35 | `run_job` | 31 | 66 | no |
| `sim/src/world.rs` | `staged_files` | 8 | `copy_staging` | 7 | 15 | no |
| `compat/src/chain.rs` | `staged_tip` | 9 | `catch_up` | 15 | 24 | no |
| `core/src/actor.rs` | `stamp_fault` | 6 | `maintain_unit` | 53 | 59 | no |
| `store-s3/src/store.rs` | `stamp_from_head` | 17 | `head_stamp` | 12 | 29 | no |
| `core/src/maintain/retain.rs` | `stamps_backwards` | 13 | `decide` | 44 | 57 | no |
| `core/src/compact.rs` | `standard` | 6 | `compact` | 8 | 14 | no |
| `core/src/source/fake.rs` | `start_generation` | 15 | `restart_in_place` | 4 | 19 | no |
| `store-s3/src/upload.rs` | `start_multipart` | 16 | `append` | 22 | 38 | no |
| `sim/src/sqlite.rs` | `start_write` | 18 | `write` | 28 | 46 | no |
| `sim/src/generate.rs` | `store_error` | 13 | `store_fault` | 31 | 44 | no |
| `core/src/plan.rs` | `straddling_file` | 15 | `as_of_outcome` | 14 | 29 | no |
| `core/src/upload.rs` | `tail_is_sound` | 31 | `list_step` | 35 | 66 | no |
| `compat/src/live_source.rs` | `take_token` | 4 | `begin_writer` | 12 | 16 | no |
| `store-s3/src/fake.rs` | `target_key` | 11 | `handle` | 25 | 36 | no |
| `core/src/recover.rs` | `tip_txid` | 6 | `recover_traced` | 44 | 50 | no |
| `core/src/recover.rs` | `unreached` | 7 | `tip_range` | 53 | 60 | no |
| `core/src/maintain/retain.rs` | `unstamped` | 11 | `decide` | 44 | 55 | no |
| `compat/src/workload.rs` | `update_items` | 17 | `apply` | 11 | 28 | no |
| `compat/src/binaries.rs` | `upstream_bin_dir` | 9 | `binary_named` | 9 | 18 | no |
| `store-s3/src/fake.rs` | `url_encode` | 13 | `list` | 42 | 55 | no |
| `core/src/compact.rs` | `verified_chain` | 19 | `write_output` | 33 | 52 | no |
| `cli/src/run.rs` | `verify_command` | 11 | `run` | 9 | 20 | no |
| `core/src/restore.rs` | `verify_end_state` | 13 | `restore_from` | 48 | 61 | no |
| `cli/src/render.rs` | `verify_json` | 37 | `done` | 31 | 68 | no |
| `core/src/retry.rs` | `wait_after` | 13 | `run` | 21 | 34 | no |
| `sim/src/run.rs` | `wal_damage` | 21 | `apply` | 47 | 68 | no |
| `ltx/src/header.rs` | `wal_origin` | 20 | `decode` | 37 | 57 | no |
| `compat/src/generate.rs` | `wal_salts` | 7 | `caught_up` | 17 | 24 | no |
| `compat/src/generate.rs` | `with_log` | 16 | `settle_replica` | 13 | 29 | no |
| `sim/src/world.rs` | `without_origin` | 3 | `strip_newest_origin` | 10 | 13 | no |
| `core/src/actor.rs` | `without_wal` | 21 | `step` | 33 | 54 | no |
| `ltx/src/ids.rs` | `zone_colon` | 6 | `zone_seconds` | 21 | 27 | no |
| `ltx/src/ids.rs` | `zone_seconds` | 21 | `parse_rfc3339` | 40 | 61 | no |

## Public functions with one call site in the workspace (158)

| File | Function | Lines | Only caller | Caller lines | Sum | Above the limit if folded |
| --- | --- | --- | --- | --- | --- | --- |
| `core/src/conformance.rs` | `pub abort_publishes_nothing` | 29 | `(not in a function)` | 0 | 29 | no |
| `sim/src/world.rs` | `pub adopt_local_state` | 12 | `wal_damage` (sim/src/run.rs) | 21 | 33 | no |
| `compat/src/manifest.rs` | `pub all_fixtures` | 12 | `(not in a function)` (compat/src/lib.rs) | 0 | 12 | no |
| `sim/src/scenario.rs` | `pub app_checkpoint_while_down` | 12 | `scenario_of` | 18 | 30 | no |
| `sim/src/world.rs` | `pub app_vacuum` | 11 | `resize` (sim/src/run.rs) | 10 | 21 | no |
| `sim/src/scenario/faults.rs` | `pub bit_flip_in_committed_frame` | 9 | `fault_scenarios` | 30 | 39 | no |
| `sim/src/scenario/faults.rs` | `pub bucket_rolled_back` | 28 | `fault_scenarios` | 30 | 58 | no |
| `compat/src/golive.rs` | `pub build_verify_helper` | 23 | `(not in a function)` (compat/src/lib.rs) | 0 | 23 | no |
| `compat/src/binaries.rs` | `pub builds_in` | 6 | `(not in a function)` (compat/src/lib.rs) | 0 | 6 | no |
| `sim/src/scenario/faults.rs` | `pub busy_on_barrier` | 10 | `fault_scenarios` | 30 | 40 | no |
| `sim/src/sqlite.rs` | `pub checkpointer` | 5 | `run_one` (sim/src/run.rs) | 20 | 25 | no |
| `core/src/conformance.rs` | `pub chunks_concatenate_and_stamp_survives` | 27 | `(not in a function)` | 0 | 27 | no |
| `sim/src/scenario.rs` | `pub cli_snapshot` | 17 | `scenario_of` | 18 | 35 | no |
| `compat/src/chain.rs` | `pub clock_ending_now` | 4 | `(not in a function)` (compat/src/lib.rs) | 0 | 4 | no |
| `sim/src/scenario/faults.rs` | `pub clock_steps_back_10s` | 14 | `clock_scenarios` | 3 | 17 | no |
| `core/src/conformance.rs` | `pub commit_error_may_leave_the_object_visible` | 25 | `(not in a function)` | 0 | 25 | no |
| `sim/src/scenario/faults.rs` | `pub commit_fails_after_publish` | 11 | `fault_scenarios` | 30 | 41 | no |
| `wal/src/scan.rs` | `pub commit_follows` | 26 | `read` (core/src/recover.rs) | 11 | 37 | no |
| `store-s3/src/multipart.rs` | `pub complete_upload` | 11 | `finish_multipart` (store-s3/src/upload.rs) | 30 | 41 | no |
| `store-s3/src/multipart.rs` | `pub completed_etag_fits` | 9 | `finish_multipart` (store-s3/src/upload.rs) | 30 | 39 | no |
| `sim/src/world.rs` | `pub copy_staging` | 7 | `meta` (sim/src/run.rs) | 33 | 40 | no |
| `compat/src/rsrestore.rs` | `pub copy_tree` | 13 | `stage` | 6 | 19 | no |
| `sim/src/scenario/faults.rs` | `pub corrupt_l1_publish_then_power_loss` | 3 | `window_scenarios` | 6 | 9 | no |
| `sim/src/scenario/faults.rs` | `pub corrupt_l9_publish_then_power_loss` | 3 | `window_scenarios` | 6 | 9 | no |
| `sim/src/store.rs` | `pub corrupting_through` | 4 | `new` (sim/src/world.rs) | 35 | 39 | no |
| `store-s3/src/multipart.rs` | `pub create_upload` | 10 | `start_multipart` (store-s3/src/upload.rs) | 16 | 26 | no |
| `sim/src/scenario/faults.rs` | `pub creds_revoked_mid_run` | 15 | `fault_scenarios` | 30 | 45 | no |
| `core/src/conformance.rs` | `pub cursor_is_exclusive` | 28 | `(not in a function)` | 0 | 28 | no |
| `sim/src/scenario/faults.rs` | `pub db_created_no_pages` | 11 | `fault_scenarios` | 30 | 41 | no |
| `core/src/conformance.rs` | `pub delete_is_idempotent` | 16 | `(not in a function)` | 0 | 16 | no |
| `sim/src/world.rs` | `pub delete_newest_staged` | 8 | `meta` (sim/src/run.rs) | 33 | 41 | no |
| `sim/src/sqlite.rs` | `pub delete_wal` | 9 | `env_fault` (sim/src/run.rs) | 30 | 39 | no |
| `sim/src/scenario/faults.rs` | `pub disk_full_on_staging` | 8 | `fault_scenarios` | 30 | 38 | no |
| `sim/src/scenario/faults.rs` | `pub eio_on_wal_read` | 13 | `fault_scenarios` | 30 | 43 | no |
| `sim/src/sqlite.rs` | `pub empty_database_once` | 5 | `env_fault` (sim/src/run.rs) | 30 | 35 | no |
| `sim/src/scenario.rs` | `pub empty_meta_boot` | 7 | `scenario_of` | 18 | 25 | no |
| `ltx/src/encoder.rs` | `pub encode_commit_zero` | 6 | `(not in a function)` | 0 | 6 | no |
| `cli/src/render.rs` | `pub error_line` | 7 | `report` (cli/src/main.rs) | 6 | 13 | no |
| `sim/src/scenario/faults.rs` | `pub etag_mismatch_on_put` | 13 | `fault_scenarios` | 30 | 43 | no |
| `cli/src/run.rs` | `pub exit_for_done` | 10 | `finish` (cli/src/main.rs) | 12 | 22 | no |
| `cli/src/run.rs` | `pub exit_for_error` | 26 | `report` (cli/src/main.rs) | 6 | 32 | no |
| `core/src/layout.rs` | `pub fallback` | 6 | `stamp_from_head` (store-s3/src/store.rs) | 17 | 23 | no |
| `compat/src/manifest.rs` | `pub fixture_dir` | 3 | `(not in a function)` (compat/src/lib.rs) | 0 | 3 | no |
| `sim/src/testkit.rs` | `pub fkey` | 6 | `put` | 12 | 18 | no |
| `sim/src/sqlite.rs` | `pub flip_bit` | 15 | `wal_damage` (sim/src/run.rs) | 21 | 36 | no |
| `core/src/conformance.rs` | `pub foreign_names_are_skipped_and_counted` | 27 | `(not in a function)` | 0 | 27 | no |
| `core/src/maintain/state.rs` | `pub forget_unlisted` | 3 | `decide` (core/src/maintain.rs) | 43 | 46 | no |
| `sim/src/scenario/faults.rs` | `pub frame_overwritten_between_scan_and_read` | 7 | `fault_scenarios` | 30 | 37 | no |
| `wal/src/testing.rs` | `pub frame_raw` | 13 | `frame_with` | 3 | 16 | no |
| `wal/src/testing.rs` | `pub frame_with` | 3 | `frame` | 4 | 7 | no |
| `core/src/fault.rs` | `pub from_read_page` | 8 | `page_fault` (core/src/sync.rs) | 9 | 17 | no |
| `core/src/fault.rs` | `pub from_wal` | 13 | `scan_fault` (core/src/sync.rs) | 16 | 29 | no |
| `sim/src/scenario/faults.rs` | `pub fsync_fails_once` | 18 | `fault_scenarios` | 30 | 48 | no |
| `core/src/maintain/config.rs` | `pub go_default` | 9 | `actor_config` (sim/src/run.rs) | 17 | 26 | no |
| `compat/src/golive.rs` | `pub go_verify` | 18 | `(not in a function)` (compat/src/lib.rs) | 0 | 18 | no |
| `sim/src/scenario/faults.rs` | `pub halt_of` | 34 | `names_fault` | 15 | 49 | no |
| `compat/src/chain.rs` | `pub hash_now` | 21 | `catch_up` | 15 | 36 | no |
| `ltx/src/frame.rs` | `pub hashes_unit` | 3 | `frame_unit` (ltx/src/decoder.rs) | 34 | 37 | no |
| `fuzzing/src/index.rs` | `pub header_spec` | 24 | `(not in a function)` | 0 | 24 | no |
| `compat/src/replica.rs` | `pub info_of` | 9 | `gap_of` (compat/src/generate.rs) | 17 | 26 | no |
| `core/src/maintain/config.rs` | `pub l0_retention` | 3 | `candidates` (core/src/maintain/retain.rs) | 49 | 52 | no |
| `core/src/verify/dry_run.rs` | `pub latest_restore` | 29 | `contents_pass` (core/src/verify.rs) | 28 | 57 | no |
| `core/src/conformance.rs` | `pub levels_are_separate` | 14 | `(not in a function)` | 0 | 14 | no |
| `store-s3/src/request.rs` | `pub list_continue` | 14 | `fetch_page` (store-s3/src/list.rs) | 30 | 44 | no |
| `core/src/conformance.rs` | `pub list_empty_level` | 8 | `(not in a function)` | 0 | 8 | no |
| `core/src/conformance.rs` | `pub listed_stamp_is_exact_or_marked` | 12 | `(not in a function)` | 0 | 12 | no |
| `core/src/conformance.rs` | `pub listing_last_modified` | 3 | `(not in a function)` (core/src/mem.rs) | 0 | 3 | no |
| `sim/src/store.rs` | `pub mark_volatile` | 3 | `commit` | 33 | 36 | no |
| `cli/src/run.rs` | `pub meaning` | 9 | `help` (cli/src/render.rs) | 30 | 39 | no |
| `sim/src/scenario/faults.rs` | `pub meta_dir_deleted_mid_run` | 29 | `fault_scenarios` | 30 | 59 | no |
| `sim/src/scenario.rs` | `pub meta_wiped_remote_kept` | 4 | `scenario_of` | 18 | 22 | no |
| `sim/src/world.rs` | `pub model_matches_sqlite` | 13 | `adopt_local_state` | 12 | 25 | no |
| `compat/src/scenario.rs` | `pub must_shrink` | 3 | `attempt` (compat/src/generate.rs) | 71 | 74 | yes |
| `compat/src/replica.rs` | `pub newest_l0` | 17 | `caught_up` (compat/src/generate.rs) | 17 | 34 | no |
| `sim/src/scenario.rs` | `pub newest_staged_file_lost` | 4 | `scenario_of` | 18 | 22 | no |
| `ltx/src/decoder.rs` | `pub next_event` | 18 | `reencode_file` (compat/src/rsrestore.rs) | 23 | 41 | no |
| `sim/src/model.rs` | `pub note_snapshot` | 8 | `note_snapshots` (sim/src/run.rs) | 47 | 55 | no |
| `sim/src/model.rs` | `pub note_stage` | 11 | `note_snapshots` (sim/src/run.rs) | 47 | 58 | no |
| `sim/src/oracle.rs` | `pub on_did` | 9 | `step` (sim/src/run.rs) | 62 | 71 | yes |
| `sim/src/model.rs` | `pub on_power_loss` | 13 | `crash` (sim/src/world.rs) | 17 | 30 | no |
| `cli/src/replica.rs` | `pub open_s3` | 5 | `open` (cli/src/run.rs) | 7 | 12 | no |
| `sim/src/sqlite.rs` | `pub our_reader_begin` | 3 | `apply` (sim/src/run.rs) | 47 | 50 | no |
| `sim/src/sqlite.rs` | `pub our_reader_end` | 3 | `apply` (sim/src/run.rs) | 47 | 50 | no |
| `core/src/maintain.rs` | `pub output_key` | 3 | `run_compact` | 34 | 37 | no |
| `sim/src/sqlite.rs` | `pub overwrite_frame_between_scan_and_read` | 13 | `env_fault` (sim/src/run.rs) | 30 | 43 | no |
| `core/src/conformance.rs` | `pub paging_at_every_limit` | 24 | `(not in a function)` | 0 | 24 | no |
| `cli/src/render.rs` | `pub paths` | 7 | `(not in a function)` (cli/src/run.rs) | 0 | 7 | no |
| `core/src/conformance.rs` | `pub pending_and_dropped_uploads_are_invisible` | 39 | `(not in a function)` | 0 | 39 | no |
| `sim/src/scenario.rs` | `pub pinned_on_zero_single_restart` | 20 | `scenario_of` | 18 | 38 | no |
| `sim/src/store.rs` | `pub power_loss` | 35 | `crash` (sim/src/world.rs) | 17 | 52 | no |
| `sim/src/scenario.rs` | `pub power_loss_wal_rewind` | 15 | `scenario_of` | 18 | 33 | no |
| `sim/src/store.rs` | `pub push_in` | 18 | `store_fault` (sim/src/run.rs) | 13 | 31 | no |
| `store-s3/src/multipart.rs` | `pub put_part` | 24 | `send_part` (store-s3/src/upload.rs) | 25 | 49 | no |
| `sim/src/scenario/faults.rs` | `pub reacquire_fails_after_checkpoint` | 13 | `fault_scenarios` | 30 | 43 | no |
| `core/src/conformance.rs` | `pub read_of_missing_key_is_not_found` | 10 | `(not in a function)` | 0 | 10 | no |
| `core/src/conformance.rs` | `pub read_spans_the_end` | 21 | `(not in a function)` | 0 | 21 | no |
| `core/src/conformance.rs` | `pub read_with_stamp_agrees_with_read_and_stamp` | 19 | `(not in a function)` | 0 | 19 | no |
| `core/src/maintain/state.rs` | `pub record_snapshot` | 15 | `snapshot_job` (core/src/actor.rs) | 35 | 50 | no |
| `sim/src/sqlite.rs` | `pub regrow_same_salts` | 3 | `wal_damage` (sim/src/run.rs) | 21 | 24 | no |
| `sim/src/sqlite.rs` | `pub regrow_without_commit_marks` | 3 | `wal_damage` (sim/src/run.rs) | 21 | 24 | no |
| `compat/src/replica.rs` | `pub rel_of` | 3 | `(not in a function)` | 0 | 3 | no |
| `sim/src/scenario/faults.rs` | `pub remote_bitrot_on_list_read` | 15 | `fault_scenarios` | 30 | 45 | no |
| `sim/src/scenario/faults.rs` | `pub remote_file_deleted_externally` | 20 | `fault_scenarios` | 30 | 50 | no |
| `sim/src/world.rs` | `pub remote_put_foreign` | 21 | `remote` (sim/src/run.rs) | 16 | 37 | no |
| `sim/src/world.rs` | `pub remote_roll_back` | 17 | `remote` (sim/src/run.rs) | 16 | 33 | no |
| `sim/src/world.rs` | `pub replace_staging` | 7 | `meta` (sim/src/run.rs) | 33 | 40 | no |
| `sim/src/seed.rs` | `pub replay_line` | 6 | `message` (sim/src/run.rs) | 9 | 15 | no |
| `sim/src/scenario/faults.rs` | `pub restart_between_steps` | 15 | `fault_scenarios` | 30 | 45 | no |
| `sim/src/scenario.rs` | `pub restart_during_read_lock_lapse` | 18 | `scenario_of` | 18 | 36 | no |
| `compat/src/manifest.rs` | `pub restore_target` | 11 | `gap_targets` (compat/src/generate.rs) | 46 | 57 | no |
| `sim/src/scenario.rs` | `pub restored_old_meta` | 13 | `scenario_of` | 18 | 31 | no |
| `core/src/maintain/state.rs` | `pub retry_at` | 12 | `job_waiter` (core/src/actor.rs) | 9 | 21 | no |
| `sim/src/sqlite.rs` | `pub rewrite_page_size` | 13 | `app_vacuum` (sim/src/world.rs) | 11 | 24 | no |
| `sim/src/sqlite.rs` | `pub rewrite_same_length` | 23 | `wal_damage` (sim/src/run.rs) | 21 | 44 | no |
| `sim/src/world.rs` | `pub run_tick` | 14 | `run_one` (sim/src/run.rs) | 20 | 34 | no |
| `sim/src/exec.rs` | `pub run_until` | 16 | `run_tick` (sim/src/world.rs) | 14 | 30 | no |
| `compat/src/manifest.rs` | `pub save` | 9 | `attempt` (compat/src/generate.rs) | 71 | 80 | yes |
| `core/src/sync.rs` | `pub scan_whole` | 16 | `run` (core/src/snapshot.rs) | 61 | 77 | yes |
| `core/src/conformance.rs` | `pub scratch_names_are_not_foreign` | 13 | `(not in a function)` | 0 | 13 | no |
| `sim/src/scenario/faults.rs` | `pub second_writer_mid_run` | 23 | `fault_scenarios` | 30 | 53 | no |
| `sim/src/scenario/faults.rs` | `pub shm_header_mid_update` | 7 | `fault_scenarios` | 30 | 37 | no |
| `sim/src/sqlite.rs` | `pub shm_mid_update` | 6 | `env_fault` (sim/src/run.rs) | 30 | 36 | no |
| `sim/src/scenario/faults.rs` | `pub sigterm_during_upload` | 22 | `fault_scenarios` | 30 | 52 | no |
| `sim/src/scenario/faults.rs` | `pub size_mismatch_on_commit` | 7 | `fault_scenarios` | 30 | 37 | no |
| `sim/src/scenario/faults.rs` | `pub staged_byte_flip_before_upload` | 29 | `fault_scenarios` | 30 | 59 | no |
| `sim/src/world.rs` | `pub staged_is_full_image` | 4 | `note_snapshots` (sim/src/run.rs) | 47 | 51 | no |
| `core/src/conformance.rs` | `pub stamp_of_a_vanished_file_is_not_found` | 13 | `(not in a function)` | 0 | 13 | no |
| `core/src/conformance.rs` | `pub stamp_of_an_overwritten_file_is_refused` | 16 | `(not in a function)` | 0 | 16 | no |
| `core/src/conformance.rs` | `pub stamp_returns_the_committed_stamp` | 21 | `(not in a function)` | 0 | 21 | no |
| `store-s3/src/store.rs` | `pub stamp_stats` | 7 | `fell_back` (cli/src/replica.rs) | 3 | 10 | no |
| `sim/src/scenario/faults.rs` | `pub store_429_retry_after` | 25 | `fault_scenarios` | 30 | 55 | no |
| `sim/src/scenario/faults.rs` | `pub store_flaky_5pct` | 34 | `fault_scenarios` | 30 | 64 | no |
| `sim/src/scenario/faults.rs` | `pub store_refuses_upload` | 16 | `fault_scenarios` | 30 | 46 | no |
| `sim/src/sqlite.rs` | `pub stranger_reader_begin` | 8 | `reader` (sim/src/run.rs) | 8 | 16 | no |
| `sim/src/world.rs` | `pub strip_newest_origin` | 10 | `meta` (sim/src/run.rs) | 33 | 43 | no |
| `sim/src/sqlite.rs` | `pub tear_wal_header` | 13 | `wal_damage` (sim/src/run.rs) | 21 | 34 | no |
| `sim/src/scenario.rs` | `pub tip_byte_flip` | 4 | `scenario_of` | 18 | 22 | no |
| `sim/src/scenario.rs` | `pub tip_no_wal_origin` | 4 | `scenario_of` | 18 | 22 | no |
| `sim/src/scenario.rs` | `pub tip_truncated` | 4 | `scenario_of` | 18 | 22 | no |
| `sim/src/scenario.rs` | `pub truncate_threshold_crossed` | 20 | `scenario_of` | 18 | 38 | no |
| `sim/src/sqlite.rs` | `pub truncate_wal_at` | 11 | `wal_damage` (sim/src/run.rs) | 21 | 32 | no |
| `core/src/conformance.rs` | `pub two_uploads_to_one_key` | 46 | `(not in a function)` | 0 | 46 | no |
| `ltx/src/conv.rs` | `pub u32_le` | 3 | `legacy_block_size` (ltx/src/frame.rs) | 19 | 22 | no |
| `ltx/src/conv.rs` | `pub u64_le` | 3 | `descriptor_tail` (ltx/src/frame.rs) | 10 | 13 | no |
| `sim/src/sqlite.rs` | `pub unlink_wal` | 9 | `env_fault` (sim/src/run.rs) | 30 | 39 | no |
| `core/src/upload.rs` | `pub upload_tick` | 32 | `(not in a function)` (core/src/lib.rs) | 0 | 32 | no |
| `sim/src/store.rs` | `pub volatile_on_late_error` | 4 | `new` (sim/src/world.rs) | 35 | 39 | no |
| `sim/src/scenario.rs` | `pub wal_deleted_while_down` | 12 | `scenario_of` | 18 | 30 | no |
| `compat/src/workload.rs` | `pub wal_frames` | 8 | `caught_up` (compat/src/generate.rs) | 17 | 25 | no |
| `sim/src/scenario.rs` | `pub wal_header_torn` | 9 | `scenario_of` | 18 | 27 | no |
| `sim/src/scenario.rs` | `pub wal_regrown_same_salts_diff_bytes` | 8 | `scenario_of` | 18 | 26 | no |
| `sim/src/scenario.rs` | `pub wal_rewritten_same_length` | 8 | `scenario_of` | 18 | 26 | no |
| `sim/src/sqlite.rs` | `pub wal_synced` | 3 | `stranger_checkpoint` (sim/src/world.rs) | 7 | 10 | no |
| `sim/src/scenario/faults.rs` | `pub wal_truncated_externally_under_lock` | 19 | `fault_scenarios` | 30 | 49 | no |
| `sim/src/scenario/faults.rs` | `pub wal_unlinked_mid_run` | 9 | `fault_scenarios` | 30 | 39 | no |
| `store-s3/src/config.rs` | `pub with_region` | 4 | `from_url` | 36 | 40 | no |
| `store-s3/src/store.rs` | `pub with_transport` | 20 | `new` | 4 | 24 | no |
| `core/src/snapshot.rs` | `pub write_snapshot` | 20 | `snapshot_job` (core/src/actor.rs) | 35 | 55 | no |
