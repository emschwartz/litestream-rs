# Fuzzing

Four cargo-fuzz targets live in `fuzz/` (outside the workspace, nightly toolchain) and call one function each in
`crates/fuzzing`: `decode` (the LTX decoder at fuzz-chosen split points, then a re-encode), `index` (the page index and
one page frame), `plan` (the restore planner over a fuzz-built listing) and `key` (file key and transaction id
parsing). The committed corpus under `fuzz/corpus/<target>/` is replayed by `crates/fuzzing/tests/corpus.rs` on the
stable toolchain, so `just check` runs every corpus file without libFuzzer.

## One-time setup

```
rustup toolchain install nightly --profile minimal
cargo install --locked cargo-fuzz
```

`just fuzz <target> <seconds>` then builds the target with the nightly toolchain and runs it. Checked with
cargo-fuzz 0.13.2: `just fuzz key 20` did 4.7 million runs in 21 seconds on one core.

## The 24-hour run (phase 1 exit)

The exit bar is that the decoders survive a 24-hour run. One libFuzzer process uses one core, so run the four
targets side by side, each for the whole day, on a machine with at least four free cores and 2 GiB of memory per
target (the recipe caps a target at 2048 MiB and inputs at 32 KiB):

```
for target in decode index plan key; do
    nohup just fuzz "$target" 86400 > "fuzz-$target.log" 2>&1 &
done
wait
```

On fewer cores, give the day to `decode` and `index` (the decoders the exit bar names) and an hour each to `plan`
and `key`. The run needs no network, no Docker and no credentials.

What a run leaves behind:

- `fuzz-<target>.log` ends with libFuzzer's final statistics (`stat::number_of_executed_units` and the rest) when
  the time ran out with no finding. Record the four run counts in the pull request that reports the run.
- New inputs that reached new coverage are added to `fuzz/corpus/<target>/`. They are untracked until committed.
- A finding stops that target and writes the input to `fuzz/artifacts/<target>/crash-<hash>` (or `oom-`,
  `timeout-`). The directory is ignored by git.

## After a clean run

Minimise each corpus, check its size, and commit what is left:

```
for target in decode index plan key; do just fuzz-cmin "$target"; done
du -sb fuzz/corpus        # the committed corpus stays at or under 2 MiB of file bytes
just check                # the corpus replay test runs every remaining file
```

A run can also update `fuzz/Cargo.lock`; restore it (`git checkout fuzz/Cargo.lock`) unless the update is wanted.

## After a finding

1. Reproduce it: `cargo +nightly fuzz run <target> --fuzz-dir fuzz fuzz/artifacts/<target>/crash-<hash>`.
2. Shrink it: `cargo +nightly fuzz tmin <target> --fuzz-dir fuzz fuzz/artifacts/<target>/crash-<hash>`.
3. Write the failing test first: the minimised bytes as a named regression test beside the code at fault
   (`crates/ltx` for a decoder, `crates/core` for the planner), then fix the root cause in its own commit.
4. Copy the minimised input into `fuzz/corpus/<target>/` so the replay test keeps it.
5. Start the 24 hours again for that target: the bar is a whole day with no finding.

`just fuzz-seed` rebuilds the seed corpus from the Go-written fixtures if a corpus directory is ever lost.
