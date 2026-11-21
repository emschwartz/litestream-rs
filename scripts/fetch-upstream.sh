#!/bin/sh
# Clones the upstream sources the port is checked against into .upstream/ (gitignored).
# Pins: the commit the research read, and the LTX library version that commit uses.
# Run from a worktree, it uses the main checkout's .upstream (as the build script and the
# compat harness read it), so a worktree never clones its own copy.
set -eu
cd "$(dirname "$0")/.."
if [ ! -d .upstream ]; then
    common=$(git rev-parse --path-format=absolute --git-common-dir)
    cd "$(dirname "$common")"
fi
mkdir -p .upstream
fetch() {
    name=$1 url=$2 rev=$3
    if [ ! -d ".upstream/$name/.git" ]; then
        git clone --quiet "$url" ".upstream/$name"
    fi
    git -C ".upstream/$name" fetch --quiet --tags origin
    git -C ".upstream/$name" checkout --quiet --detach "$rev"
    echo "$name @ $(git -C ".upstream/$name" rev-parse --short HEAD)"
}
fetch litestream https://github.com/benbjohnson/litestream 041a8c382c63
fetch ltx https://github.com/superfly/ltx v0.5.3
