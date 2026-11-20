#!/bin/sh
# Clones the upstream sources the port is checked against into .upstream/ (gitignored).
# Pins: the commit the research read, and the LTX library version that commit uses.
set -eu
cd "$(dirname "$0")/.."
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
