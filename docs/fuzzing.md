# Fuzzing

Phase 1's exit asks that the decoders survive a 24-hour fuzz run. This is how to run it and what
counts as passing. It has not been run yet: a cloud session is too short for it.

## Targets

Each target is a one-line `fuzz/fuzz_targets/<target>.rs` calling into `crates/fuzzing`, and
`crates/fuzzing/tests/corpus.rs` replays the committed corpus in `just check`.

| Target | What must hold |
| --- | --- |
| `decode` | LTX bytes split at fuzz-chosen points into `Decoder::feed`: no panic, the same events for any split, and a `Verified` file re-encodes and decodes to the same pages |
| `index` | `PageIndex::decode` and `decode_frame` on fuzz bytes: no panic, no allocation beyond one page and one frame |
| `plan` | an inventory and a target from fuzz bytes: a plan starts at TXID 1, is contiguous and ends exactly at the target; the lazy planner agrees |
| `key` | `FileKey` and `Txid` parsing: accepted input prints back byte for byte |

PLAN.md section 4 also names the WAL parser (`lsr-wal`), which has no target yet.

## Setup

```bash
rustup toolchain install nightly --profile minimal
```

```bash
cargo install --locked cargo-fuzz
```

Checked with cargo-fuzz 0.13.2. The fuzz crate has its own `fuzz/Cargo.lock`, which `cargo fuzz`
updates without `--locked`; a diff there after a run means a workspace dependency changed and the
lockfile should be committed.

## The 24-hour run

One process per target, each on its own core (`just fuzz` caps each at 2 GiB RSS, so four need
about 8 GiB free). On fewer than four cores, give the day to `decode` and `index`, the decoders the
exit names, and an hour each to `plan` and `key`. Run it from the main checkout on a machine that
stays up:

```bash
mkdir -p target/fuzz-logs && for t in decode index plan key; do nohup just fuzz "$t" 86400 > "target/fuzz-logs/$t.log" 2>&1 & done
```

A target passes when its log ends with `Done <n> runs in 86400 second(s)` and the `stat::` lines,
and `fuzz/artifacts/<target>/` holds no `crash-`, `leak-`, `timeout-` or `oom-` file. Record each
target's commit, run count and `stat::new_units_added` in the pull request that closes the item.

## After the run

- A finding is replayed with `cargo +nightly fuzz run <target> --fuzz-dir fuzz fuzz/artifacts/<target>/<file>`.
  A crash in `crates/ltx` is fixed at the root, with the input as a named regression test in its own
  commit (phase 1 task 7). A crash anywhere else is reported with its reproducer first.
  Shrink the input first with `cargo +nightly fuzz tmin <target> --fuzz-dir fuzz fuzz/artifacts/<target>/<file>`,
  copy the shrunk input into `fuzz/corpus/<target>/` so the replay test keeps it, and start that
  target's 24 hours again: the bar is a whole day with no finding.
- The run writes new inputs into `fuzz/corpus/<target>/`. Minimise each target with
  `just fuzz-cmin <target>` before committing any of them. The committed corpus is about 0.8 MiB
  today and stays under 2 MiB in total (phase 1 task 7); drop the new inputs rather than go over.
- Run `just check` afterwards: the corpus replay test reads every committed input.
