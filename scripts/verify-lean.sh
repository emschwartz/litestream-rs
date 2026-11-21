#!/usr/bin/env bash
# Regenerates the Lean translation of the restore planner's extension step with
# Charon and Aeneas, fails if it differs from the committed copy in
# verification/lean/LsrCore/, then checks the proofs with `lake build` and
# fails if any theorem rests on an axiom other than Lean's own three and the
# one stated sort axiom. Not part of `just check` (verification/lean/README.md).
#
# Pins: Charon and Aeneas nightly-2026.11.21-2e3d448 (Aeneas commit 2e3d448,
# which also pins the Lean library in verification/lean/lakefile.toml); Charon
# drives the Rust toolchain nightly-2026-09-17 through rustup; Lean v4.36.0
# through elan (verification/lean/lean-toolchain). charon and aeneas come from
# AENEAS_BIN when set, else from PATH. `--regen` rewrites the committed
# translation instead of comparing against it.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
proofs="$root/verification/lean"
if [ -n "${AENEAS_BIN:-}" ]; then
    PATH="$AENEAS_BIN:$PATH"
fi
want="nightly-2026.11.21-2e3d448"
have="$(aeneas -version 2>/dev/null || true)"
if [ "$have" != "aeneas $want" ]; then
    echo "verify-lean: want aeneas $want on PATH or in AENEAS_BIN, found '${have:-nothing}'" >&2
    exit 1
fi
regen=0
if [ "${1:-}" = "--regen" ]; then
    regen=1
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
# lsr_ltx's ids are translated from source; the rest of std stays opaque and
# is modelled by hand in TypesExternal.lean and FunsExternal.lean.
if ! (cd "$root" && charon cargo --preset=aeneas \
    --start-from 'lsr_core::plan::extend' --include 'lsr_ltx::ids::_' \
    --dest-file "$tmp/lsr_core.llbc" -- -p lsr-core) >"$tmp/charon.log" 2>&1; then
    cat "$tmp/charon.log" >&2
    exit 1
fi
if ! aeneas -backend lean -split-files -abort-on-error -no-progress-bar \
    -dest "$tmp/out" -subdir LsrCore "$tmp/lsr_core.llbc" >"$tmp/aeneas.log" 2>&1; then
    cat "$tmp/aeneas.log" >&2
    exit 1
fi
gen="$tmp/out/LsrCore"

# Aeneas names both `Copy` parents of std's `ZeroablePrimitive` trait
# `markerCopyInst`, which Lean rejects as a duplicate field. Rename the second
# one, and fail when the pattern stops matching so a fixed Aeneas is noticed.
rename() {
    file=$1 from=$2 to=$3
    if [ "$(grep -cxF -- "$from" "$file")" != 1 ]; then
        echo "verify-lean: expected one line '$from' in $(basename "$file"); has Aeneas changed?" >&2
        exit 1
    fi
    FROM="$from" TO="$to" perl -i -pe 'chomp; $_ = $ENV{TO} if $_ eq $ENV{FROM}; $_ .= "\n"' "$file"
}
rename "$gen/Types.lean" '  markerCopyInst : core.marker.Copy Self_NonZeroInner' \
    '  markerCopyInst1 : core.marker.Copy Self_NonZeroInner'
rename "$gen/Types.lean" '  (parentClauses := ["markerCopyInst", "markerCopyInst"])]' \
    '  (parentClauses := ["markerCopyInst", "markerCopyInst1"])]'
rename "$gen/Funs.lean" '  markerCopyInst := core.num.niche_types.NonZeroU64Inner.Insts.CoreMarkerCopy' \
    '  markerCopyInst1 := core.num.niche_types.NonZeroU64Inner.Insts.CoreMarkerCopy'

if grep -nE '^(axiom|opaque)|sorry' "$gen/Types.lean" "$gen/Funs.lean"; then
    echo "verify-lean: the translation has an axiom or sorry; something became opaque" >&2
    exit 1
fi
# The comparison skips the `Source:` comment lines, which carry line numbers:
# an edit elsewhere in plan.rs moves them without changing what is proved.
strip() { grep -v "^ *Source: '" "$1"; }
for file in Types.lean Funs.lean; do
    if [ "$regen" = 1 ]; then
        cp "$gen/$file" "$proofs/LsrCore/$file"
    elif ! diff -u <(strip "$proofs/LsrCore/$file") <(strip "$gen/$file"); then
        echo "verify-lean: LsrCore/$file is stale; rerun with --regen and fix the proofs" >&2
        exit 1
    fi
done

cd "$proofs"
if grep -rnw 'sorry\|admit' --include='*.lean' LsrCore LsrCore.lean; then
    echo "verify-lean: a proof is unfinished" >&2
    exit 1
fi
lake build
# Every main theorem may rest on Lean's three axioms and the sort axiom only.
if ! lake env lean LsrCore/Axioms.lean >"$tmp/axioms.txt" 2>&1; then
    cat "$tmp/axioms.txt" >&2
    exit 1
fi
cat "$tmp/axioms.txt"
allowed='propext|Classical.choice|Quot.sound|lsr_core.plan.sort_by_min_spec'
reports="$(tr '\n' ' ' <"$tmp/axioms.txt" | grep -oE 'axioms: \[[^]]*\]' || true)"
if [ "$(printf '%s\n' "$reports" | grep -c .)" != 2 ]; then
    echo "verify-lean: expected an axiom report for each of the two theorems" >&2
    exit 1
fi
if printf '%s\n' "$reports" | sed -E 's/axioms: \[|\]//g' | tr ',' '\n' \
    | sed -E 's/^ +| +$//g' | grep -vE "^($allowed)$" | grep .; then
    echo "verify-lean: a theorem rests on an axiom outside the trusted base" >&2
    exit 1
fi
echo "verify-lean: translation fresh, proofs checked"
