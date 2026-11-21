# Function-length checkpoint (decision D5)

Phase 1's exit asks for every function with exactly one caller, and a ruling on whether the 70-line
limit (`too-many-lines-threshold = 70` in `clippy.toml`) is what creates them. Measured at `34016b9`.

**Ruling: keep 70.** Almost every single-caller function would fit inside its caller without
breaking the limit, so the limit is not what made them. The few it does hold apart are named steps
of a long sequence, not fragments.

## How it was measured

```bash
rust-analyzer lsif . > /tmp/lsif.json
python3 -I scripts/single-callers.py /tmp/lsif.json . > docs/function-length/single-callers.tsv
```

`scripts/single-callers.py` counts callers from rust-analyzer's reference index and lengths the way
clippy counts them (body lines, no blanks or comments). Its header lists what it leaves out: test
code, `use` re-exports, and trait methods and trait-impl methods, whose callers go through the
trait. The full list is [`function-length/single-callers.tsv`](function-length/single-callers.tsv),
one row per function, with the caller and both lengths. As a check on the length count, the
longest function the script finds is 67 lines; clippy rejects anything above 70.

## Numbers

| Functions (non-test, not trait items) | Count |
| --- | ---: |
| No caller outside tests (public API used by other crates' tests, or test support) | 170 |
| Exactly one caller | 699 |
| Two or more callers | 689 |
| Total | 1558 |

Of the 699 single-caller functions, **662 (95%) could be pasted into their caller and the caller
would still be at most 70 lines.** For those the limit played no part: they exist because they name
a step, are a method on a type, or are public. By crate: core 222, sim 182, store-s3 96, compat 64,
ltx 52, cli 38, wal 17, store-fs 13, fuzzing 11, local 4.

The workspace has one `#[expect(clippy::too_many_lines)]`: `finding_parts` in
`crates/cli/src/render.rs`, a match with one arm per `Finding` variant that must stay exhaustive.

## The 37 the limit holds apart

Pasting any of these into its caller would take the caller over 70 lines. Lengths are body lines.

| Function | Length | Caller | Caller length | Combined |
| --- | ---: | --- | ---: | ---: |
| `crates/cli/src/args.rs:381` `scan` | 33 | `parse` | 38 | 71 |
| `crates/compat/src/manifest.rs:198` `save` | 7 | `attempt` | 64 | 71 |
| `crates/ltx/src/index.rs:123` `decode` | 50 | `index` | 21 | 71 |
| `crates/sim/src/resources.rs:161` `per_tick` | 12 | `step` | 60 | 72 |
| `crates/compat/src/workload.rs:86` `apply` | 9 | `attempt` | 64 | 73 |
| `crates/sim/src/run.rs:666` `env_fault` | 28 | `apply` | 45 | 73 |
| `crates/cli/src/render.rs:377` `verify_text` | 45 | `done` | 29 | 74 |
| `crates/core/src/recover.rs:251` `agrees` | 7 | `load` | 67 | 74 |
| `crates/sim/src/run.rs:356` `note_tick` | 14 | `step` | 60 | 74 |
| `crates/core/src/recover.rs:696` `same_generation` | 33 | `recover_traced` | 42 | 75 |
| `crates/compat/src/generate.rs:226` `attempt` | 64 | `generate` | 12 | 76 |
| `crates/core/src/maintain.rs:427` `plan_level` | 42 | `decide` | 34 | 76 |
| `crates/core/src/maintain/retain.rs:373` `candidates` | 41 | `decide` | 35 | 76 |
| `crates/sim/src/run.rs:727` `meta` | 31 | `apply` | 45 | 76 |
| `crates/sim/src/scenario/faults.rs:387` `throttle_waits` | 53 | `store_429_retry_after` | 23 | 76 |
| `crates/core/src/sync.rs:582` `created_at` | 14 | `run` | 63 | 77 |
| `crates/core/src/verify.rs:718` `read` | 46 | `file` | 31 | 77 |
| `crates/compat/src/replicator.rs:45` `start` | 14 | `attempt` | 64 | 78 |
| `crates/core/src/verify.rs:504` `name_findings` | 49 | `verify` | 29 | 78 |
| `crates/sim/src/run.rs:262` `run_one` | 18 | `step` | 60 | 78 |
| `crates/core/src/recover.rs:361` `newest_level_zero` | 12 | `load` | 67 | 79 |
| `crates/compat/src/generate.rs:90` `tail` | 16 | `attempt` | 64 | 80 |
| `crates/compat/src/generate.rs:367` `record_targets` | 17 | `attempt` | 64 | 81 |
| `crates/compat/src/scenario.rs:84` `steps` | 17 | `attempt` | 64 | 81 |
| `crates/wal/src/scan.rs:404` `check_cursor` | 34 | `next_tx` | 47 | 81 |
| `crates/compat/src/workload.rs:63` `create` | 19 | `attempt` | 64 | 83 |
| `crates/core/src/recover.rs:336` `match_last_page` | 16 | `load` | 67 | 83 |
| `crates/compat/src/generate.rs:298` `wait_for_new_snapshot` | 20 | `attempt` | 64 | 84 |
| `crates/compat/src/replicator.rs:88` `stop` | 21 | `attempt` | 64 | 85 |
| `crates/sim/src/run.rs:784` `apply` | 45 | `run_ops` | 42 | 87 |
| `crates/core/src/recover.rs:152` `load` | 67 | `boot_recover` | 21 | 88 |
| `crates/compat/src/generate.rs:323` `publish_replica` | 36 | `attempt` | 64 | 100 |
| `crates/core/src/sync.rs:248` `of` | 38 | `run` | 63 | 101 |
| `crates/sim/src/oracle.rs:227` `covers` | 62 | `check` | 43 | 105 |
| `crates/sim/src/run.rs:286` `note_snapshots` | 45 | `step` | 60 | 105 |
| `crates/core/src/recover.rs:394` `scan_pages` | 42 | `load` | 67 | 109 |
| `crates/core/src/recover.rs:269` `tip_range` | 46 | `load` | 67 | 113 |

What they are:

- **A long sequence cut at its steps.** `recover.rs` `load` (67 lines) calls `tip_range`,
  `scan_pages`, `match_last_page`, `newest_level_zero` and `Range::agrees`; compat's `attempt`
  (64) calls `start`, `stop`, `tail`, `steps`, `save` and the snapshot waits; the simulator's
  `step` (60) and `apply` (45) dispatch to one function per operation. Each piece has its own name,
  doc comment and error path. Raising the limit to 100 would allow merging some of them back, and
  the merged function would be harder to read, not easier.
- **Methods and public functions.** `Manifest::save`, `resources::per_tick`, the
  `verify` renderers: they would be separate functions at any limit.
- **None is a fragment with no name of its own** (a `part_two`, or a helper that takes half the
  caller's locals as arguments). The longest parameter list in the table is `sync.rs` `created_at`
  with six, which builds one LTX header from its inputs.

## When to look again

Rerun the script at each phase exit. Raise the limit if the share of single-caller functions that
fit in their caller drops well below 95%, if helpers named for their position (`_part`, `_rest`,
`_inner`) appear (today there is one, `lsr-wal`'s `read_page_inner`, which exists so `read_page`
can zero the buffer on every error path), or if `#[expect(clippy::too_many_lines)]` spreads beyond exhaustive matches.
