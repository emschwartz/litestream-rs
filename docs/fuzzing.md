# Fuzzing

Phase 1's exit asks that the decoders survive a 24-hour fuzz run. This is how to run it and what
counts as passing. It has not been run yet: a cloud session is too short for it.

## Targets

Each target is a one-line `fuzz/fuzz_targets/<target>.rs` calling into `crates/fuzzing`, and
`crates/fuzzing/tests/corpus.rs` replays the committed corpus in `just check`. `just fuzz-seed`
writes the seeds: the LTX targets' from the Go-written fixtures, the `wal` target's from WALs built
with `lsr_wal::testing::WalBuilder` and a few written by SQLite.

| Target | What must hold |
| --- | --- |
| `decode` | LTX bytes split at fuzz-chosen points into `Decoder::feed`: no panic, the same events for any split, and a `Verified` file re-encodes and decodes to the same pages |
| `index` | `PageIndex::decode` and `decode_frame` on fuzz bytes: no panic, no allocation beyond one page and one frame (both exist only with `lsr-ltx`'s `testing` feature: no checksum covers a page read out of order) |
| `plan` | an inventory and a target from fuzz bytes: a plan starts at TXID 1, is contiguous and ends exactly at the target; the lazy planner agrees |
| `key` | `FileKey` and `Txid` parsing: accepted input prints back byte for byte |
| `wal` | the SQLite WAL reader (`lsr-wal`) on any bytes, at a page size and byte order the input may pick, with frame checksums optionally re-chained so mutated fields get past them: `WalHeader::decode` and `WalReader::open` agree; no panic, no scan step without progress, no byte read twice by one scan; a truncated copy under `FirstInvalidFrame` gives exactly the whole scan's transactions that end before the cut; a committed end gives exactly those before it, reads nothing at or past it, ends clean at a boundary and fails anywhere else; an end or a resume of another generation is `Restarted`; `resume` at every boundary returns the scan's own cursor and scans on to the same rest; `PageMap` equals a plain model and never holds a page above the last commit; every scanned page reads back to its digest, and after one page byte flips that page is `ChangedSinceScan`, a new scan stops right before its transaction and a committed end past it is `CorruptBelowCommitted` at its frame |

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

## The nightly run

`.github/workflows/nightly.yml` runs `just fuzz <target> 600` for `decode`, `index` and `wal` (the
LTX and WAL decoders) every night and on demand, one job per target, from the committed corpus,
on nightly Rust. A target that finds a crash, leak, timeout or out-of-memory input fails its job
and uploads `fuzz/artifacts/<target>/` as the artifact `fuzz-<target>-<run id>`. The new inputs it
writes stay on the runner; the corpus only grows by the minimised commits below.

## The 24-hour run

Five processes, one per target, each on its own core (`just fuzz` caps each at 2 GiB RSS, so five need
about 10 GiB free). Run it from the main checkout on a machine that stays up:

```bash
mkdir -p target/fuzz-logs && for t in decode index plan key wal; do nohup just fuzz "$t" 86400 > "target/fuzz-logs/$t.log" 2>&1 & done
```

A target passes when its log ends with `Done <n> runs in 86400 second(s)` and the `stat::` lines,
and `fuzz/artifacts/<target>/` holds no `crash-`, `leak-`, `timeout-` or `oom-` file. Record each
target's commit, run count and `stat::new_units_added` in the pull request that closes the item.

## After the run

- A finding is replayed with `cargo +nightly fuzz run <target> --fuzz-dir fuzz fuzz/artifacts/<target>/<file>`.
  A crash in `crates/ltx` is fixed at the root, with the input as a named regression test in its own
  commit (phase 1 task 7). A crash anywhere else is reported with its reproducer first.
- The run writes new inputs into `fuzz/corpus/<target>/`. Minimise each target with
  `just fuzz-cmin <target>` before committing any of them; minimise `wal` with
  `just fuzz-cmin wal -max_len=16384`, which keeps every coverage edge at about half the bytes (its
  inputs are whole WALs). The committed corpus is about 1.3 MiB today (0.5 MiB of it `wal`) and
  stays under 2 MiB in total (phase 1 task 7); drop the new inputs rather than go over.
- Run `just check` afterwards: the corpus replay test reads every committed input.
