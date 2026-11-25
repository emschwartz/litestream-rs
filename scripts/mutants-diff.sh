#!/usr/bin/env bash
# Mutation-tests only the lines a branch changed in lsr-core and lsr-ltx (docs/mutants.md).
# Fails when a mutant in a changed line is missed (cargo-mutants exit 2) or the unmutated tests
# fail (4). A timeout (3) is reported but passes: it is a loop that spins, which the per-test
# limit in .config/nextest.toml already turns into a failing test (docs/mutants.md, "Detected by
# non-termination"). Documented equivalent mutants live in docs/mutants-escapes.txt.
#   scripts/mutants-diff.sh [base-ref]   (default origin/main)
# MUTANTS_SHARD=k/n runs the k-th of n disjoint slices of the mutants (CI runs four in parallel).
set -euo pipefail
cd "$(dirname "$0")/.."
base="${1:-origin/main}"
out="target/mutants-diff"
mkdir -p "$out"
git diff "$base"...HEAD -- crates/core crates/ltx >"$out/changes.diff"
if ! grep -q '^+++ b/.*\.rs$' "$out/changes.diff"; then
    echo "mutants-diff: no Rust change in crates/core or crates/ltx"
    exit 0
fi
escapes=()
while IFS= read -r line; do
    case "$line" in '' | '#'*) continue ;; esac
    escapes+=(--exclude-re "$line")
done <docs/mutants-escapes.txt
status=0
# Four jobs, a 10-minute ceiling per mutant, and a hard stop for the whole run (the CI job
# also has timeout-minutes); `timeout` ends the run with 124, which fails the job.
# Two guards keep that stop real on a machine short of memory. `--kill-after` follows the
# TERM with a KILL (a thrashing run ignored the TERM and held the job to its limit). The
# `ulimit -v` caps every process's address space, so a mutant that allocates without bound
# fails its test with an allocation error, a caught mutant, instead of taking the machine down;
# the default (6 GiB) is eight times the largest honest test (about 750 MiB resident), and macOS
# does not honor it (the cap is skipped there).
vmem_kb="${MUTANTS_VMEM_KB:-6291456}"
if ! ulimit -v "$vmem_kb" 2>/dev/null; then
    echo "mutants-diff: no address-space cap on this OS"
fi
timeout --kill-after=60 "${MUTANTS_DIFF_BUDGET:-2400}" cargo mutants \
    --in-diff "$out/changes.diff" -p lsr-core -p lsr-ltx \
    --test-tool nextest --jobs "${MUTANTS_JOBS:-2}" --copy-vcs true \
    --minimum-test-timeout 60 --output "$out" \
    ${MUTANTS_SHARD:+--shard "$MUTANTS_SHARD"} "${escapes[@]}" || status=$?
case "$status" in
    0) echo "mutants-diff: every mutant in the changed lines is caught" ;;
    3) echo "mutants-diff: some mutants timed out (a spinning loop); see $out/mutants.out/timeout.txt" ;;
    2) echo "mutants-diff: MISSED mutants in changed lines (see $out/mutants.out/missed.txt)." >&2
       echo "mutants-diff: kill each with a test, or, if no input can tell it from the original," >&2
       echo "mutants-diff: add a regex to docs/mutants-escapes.txt and a row to docs/mutants.md." >&2
       exit 1 ;;
    124) echo "mutants-diff: budget exhausted before a verdict; failing, since nothing was proven" >&2
         exit 1 ;;
    *) echo "mutants-diff: cargo-mutants exited $status" >&2; exit 1 ;;
esac