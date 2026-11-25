#!/usr/bin/env bash
# Mutation-tests only the lines a branch changed in lsr-core and lsr-ltx (docs/mutants.md).
# Fails when a mutant in a changed line is missed (cargo-mutants exit 2, or 3 with a missed list:
# a timeout hides the 2) or the unmutated tests fail (4). A timeout (3) alone is reported but
# passes: it is a loop that spins, which the per-test limit in .config/nextest.toml already turns
# into a failing test (docs/mutants.md, "Detected by non-termination"). Documented equivalent
# mutants live in docs/mutants-escapes.txt.
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
timeout "${MUTANTS_DIFF_BUDGET:-2400}" cargo mutants \
    --in-diff "$out/changes.diff" -p lsr-core -p lsr-ltx \
    --test-tool nextest --jobs "${MUTANTS_JOBS:-2}" --copy-vcs true \
    --minimum-test-timeout 60 --output "$out" \
    ${MUTANTS_SHARD:+--shard "$MUTANTS_SHARD"} "${escapes[@]}" || status=$?
# cargo-mutants exits 3 when any mutant timed out, whether or not others were missed, so the
# missed list is read, not inferred from the exit code.
missed() {
    echo "mutants-diff: MISSED mutants in changed lines (see $out/mutants.out/missed.txt)." >&2
    echo "mutants-diff: kill each with a test, or, if no input can tell it from the original," >&2
    echo "mutants-diff: add a regex to docs/mutants-escapes.txt and a row to docs/mutants.md." >&2
    exit 1
}
case "$status" in
    0) echo "mutants-diff: every mutant in the changed lines is caught" ;;
    3) echo "mutants-diff: some mutants timed out (a spinning loop); see $out/mutants.out/timeout.txt"
       if [ -s "$out/mutants.out/missed.txt" ]; then missed; fi ;;
    2) missed ;;
    124) echo "mutants-diff: budget exhausted before a verdict; failing, since nothing was proven" >&2
         exit 1 ;;
    *) echo "mutants-diff: cargo-mutants exited $status" >&2; exit 1 ;;
esac
