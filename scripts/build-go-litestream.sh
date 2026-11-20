#!/bin/sh
# Builds the pinned Go litestream binaries the compat harness runs, from the read-only
# clone in .upstream/litestream, into .upstream/bin/litestream-<version> (gitignored).
# Each version is exported with `git archive` into a scratch directory, so the clone's
# checkout is never touched. Sources are built exactly as tagged; the only accommodations
# are environment ones (the toolchain's own Go version, CGO for the mattn sqlite driver).
set -eu
cd "$(dirname "$0")/.."
root=$(pwd)
# A worktree has no .upstream of its own; the main checkout's is shared.
if [ ! -d "$root/.upstream" ]; then
    common=$(git rev-parse --path-format=absolute --git-common-dir)
    root=$(dirname "$common")
fi
src="$root/.upstream/litestream"
out="$root/.upstream/bin"
# Tags the matrix covers, plus the pinned HEAD the research read. "head" is named by its
# short commit so the binary name says what it is.
versions="${*:-v0.5.0 v0.5.15 v0.5.16 v0.5.18 041a8c3}"
[ -d "$src/.git" ] || { echo "run 'just upstream' first" >&2; exit 1; }
mkdir -p "$out"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
for v in $versions; do
    bin="$out/litestream-$v"
    if [ -x "$bin" ]; then
        echo "$v: present"
        continue
    fi
    rev=$(git -C "$src" rev-parse --verify --quiet "$v^{commit}") || {
        echo "$v: not in the clone" >&2
        exit 1
    }
    dir="$scratch/$v"
    mkdir -p "$dir"
    git -C "$src" archive "$rev" | tar -x -C "$dir"
    # GOFLAGS=-mod=mod lets Go fill go.sum entries an old tag lacked; no source is patched.
    (cd "$dir" && CGO_ENABLED=1 GOFLAGS=-mod=mod go build -trimpath -o "$bin" ./cmd/litestream)
    echo "$v: built $(git -C "$src" rev-parse --short "$rev")"
done
