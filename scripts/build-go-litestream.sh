#!/bin/sh
# Builds the Go litestream binaries the compat harness runs, from the read-only clone in
# .upstream/litestream, into .upstream/bin/litestream-<name> (gitignored), and beside each
# the read-replica VFS: upstream's cmd/litestream-vfs as a SQLite loadable extension,
# .upstream/bin/litestream-vfs-<name>.so, built the way upstream's Makefile target
# vfs-linux-amd64 builds it (a Go c-archive linked with src/litestream-vfs.c). Before v0.5.3
# the VFS reads S3 only (it has no LITESTREAM_REPLICA_URL, so no file replica) and is not
# built. Each version is exported with `git archive` into a scratch directory, so the clone's
# checkout is never touched. Sources are built exactly as tagged; the only accommodations are
# environment ones (the toolchain's own Go version, CGO for the mattn sqlite driver).
#
# Names: a tag or commit; `releases` is every tag v0.5.N with no pre-release suffix;
# `main` is origin/main's tip, always rebuilt, named litestream-main (the checkout stays on
# the pin). Every name is tried; the exit status is 1 if any failed, with the last error line
# of each failure printed as "<name>: FAILED <line>" (or "<name>: vfs FAILED <line>").
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
[ -d "$src/.git" ] || { echo "run 'just upstream' first" >&2; exit 1; }
# The pinned set: the tags the committed fixtures cover, plus the pinned HEAD the research
# read, named by its short commit so the binary name says what it is.
requested="${*:-v0.5.0 v0.5.15 v0.5.16 v0.5.18 041a8c3}"
versions=""
for v in $requested; do
    case $v in
    releases) versions="$versions $(git -C "$src" tag --list 'v0.5.*' | grep -E '^v0\.5\.[0-9]+$' | sort -V | tr '\n' ' ')" ;;
    *) versions="$versions $v" ;;
    esac
done
mkdir -p "$out"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
failed=0
# Exports `rev` into the scratch directory, once per name; sets `dir`.
export_rev() {
    dir="$scratch/$1"
    if [ ! -d "$dir" ]; then
        mkdir -p "$dir"
        git -C "$src" archive "$2" | tar -x -C "$dir"
    fi
}
build() {
    v=$1 rev=$2 bin=$3
    export_rev "$v" "$rev"
    # GOFLAGS=-mod=mod lets Go fill go.sum entries an old tag lacked; no source is patched.
    if (cd "$dir" && CGO_ENABLED=1 GOFLAGS=-mod=mod go build -trimpath -o "$bin" ./cmd/litestream) >"$scratch/$v.log" 2>&1; then
        echo "$v: built $(git -C "$src" rev-parse --short "$rev")"
    else
        echo "$v: FAILED $(grep -v '^$' "$scratch/$v.log" | tail -n 1)"
        failed=1
    fi
}
build_vfs() {
    v=$1 rev=$2 so=$3
    if ! git -C "$src" grep -q LITESTREAM_REPLICA_URL "$rev" -- cmd/litestream-vfs/main.go 2>/dev/null; then
        echo "$v: vfs reads S3 only, not built"
        return
    fi
    export_rev "$v" "$rev"
    # The header cgo writes beside the archive is the one src/litestream-vfs.c includes.
    if (cd "$dir" &&
        CGO_ENABLED=1 GOFLAGS=-mod=mod go build -trimpath -tags vfs,SQLITE3VFS_LOADABLE_EXT \
            -buildmode=c-archive -o vfs.a ./cmd/litestream-vfs &&
        cp vfs.h src/litestream-vfs.h &&
        gcc -DSQLITE3VFS_LOADABLE_EXT -fPIC -shared -o "$so.tmp" src/litestream-vfs.c vfs.a \
            -lpthread -ldl -lm &&
        mv "$so.tmp" "$so") >"$scratch/$v.vfs.log" 2>&1; then
        echo "$v: vfs built $(git -C "$src" rev-parse --short "$rev")"
    else
        echo "$v: vfs FAILED $(grep -v '^$' "$scratch/$v.vfs.log" | tail -n 1)"
        failed=1
    fi
}
for v in $versions; do
    if [ "$v" = main ]; then
        # Upstream moves: fetch, and rebuild every time.
        git -C "$src" fetch --quiet origin main
        rev=$(git -C "$src" rev-parse origin/main)
        build main "$rev" "$out/litestream-main"
        build_vfs main "$rev" "$out/litestream-vfs-main.so"
        continue
    fi
    rev=$(git -C "$src" rev-parse --verify --quiet "$v^{commit}") || {
        echo "$v: not in the clone" >&2
        failed=1
        continue
    }
    bin="$out/litestream-$v"
    if [ -x "$bin" ]; then
        echo "$v: present"
    else
        build "$v" "$rev" "$bin"
    fi
    so="$out/litestream-vfs-$v.so"
    if [ -f "$so" ]; then
        echo "$v: vfs present"
    else
        build_vfs "$v" "$rev" "$so"
    fi
done
exit "$failed"
