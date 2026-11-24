#!/usr/bin/env bash
# Builds one lsr binary per patch in crates/compat/bench-patches/, for the snapshot-impact
# benchmark (just bench-snapshot-impact, issue 315). Each patch is applied to a scratch worktree
# of HEAD, exactly as just lazyfs-plant does for a plant, and built in release mode into
# <out>/<patch name>/lsr. The patches are prototypes of remedies the engine does not have; they
# are never applied to the tree itself.
#
#   scripts/build-bench-arms.sh <out dir>
set -euo pipefail

out=${1:?usage: build-bench-arms.sh <out dir>}
mkdir -p "$out"
out=$(cd "$out" && pwd)
# One target directory for every arm, so the third-party crates compile once.
target="${LSR_BENCH_ARMS_TARGET:-$PWD/target/bench-arms}"

shopt -s nullglob
patches=(crates/compat/bench-patches/*.patch)
if [ "${#patches[@]}" -eq 0 ]; then
    echo "build-bench-arms: no patches under crates/compat/bench-patches" >&2
    exit 1
fi
for patch in "${patches[@]}"; do
    name=$(basename "$patch" .patch)
    tree=".worktrees/bench-arm-$name"
    git worktree remove --force "$tree" 2>/dev/null || true
    git worktree add --detach "$tree" HEAD >/dev/null
    (
        trap 'git worktree remove --force "$tree"' EXIT
        git -C "$tree" apply "$PWD/$patch"
        CARGO_TARGET_DIR="$target" cargo build --release --locked -p lsr-cli --bin lsr --manifest-path "$tree/Cargo.toml"
    )
    mkdir -p "$out/$name"
    cp "$target/release/lsr" "$out/$name/lsr"
    echo "build-bench-arms: $name -> $out/$name/lsr ($(shasum -a 256 "$out/$name/lsr" | cut -c1-16))"
done
