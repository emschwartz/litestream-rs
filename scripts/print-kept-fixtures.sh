#!/usr/bin/env bash
# Prints what a failed alternation run kept (/tmp/lsr-alternation-*), for a CI job whose
# artifacts cannot be downloaded where the failure is read: the WAL and wal-index headers in
# hex, then the database, its WAL and its wal-index, each gzipped and base64-encoded between
# BEGIN and END markers, so the files can be rebuilt from the job log alone
# (`base64 -d | gunzip`). A file whose gzipped size passes LIMIT is named with its size and
# sha256 instead. Prints nothing when no run kept a fixture.
set -euo pipefail

LIMIT=${LSR_DUMP_LIMIT:-6000000}

shopt -s nullglob
for dir in /tmp/lsr-alternation-*/; do
    echo "== kept fixture ${dir}"
    ls -la "$dir"
    for name in app.db-wal app.db-shm; do
        if [ -f "$dir/$name" ]; then
            echo "-- first bytes of ${name}"
            od -A d -t x1 -N 136 "$dir/$name"
        fi
    done
    for name in app.db-wal app.db-shm app.db; do
        file="$dir/$name"
        [ -f "$file" ] || continue
        packed=$(mktemp)
        gzip -9c "$file" > "$packed"
        size=$(stat -c %s "$file")
        sum=$(sha256sum "$file" | cut -d' ' -f1)
        if [ "$(stat -c %s "$packed")" -gt "$LIMIT" ]; then
            echo "-- ${dir}${name}: ${size} bytes, sha256 ${sum}, too large to print"
        else
            echo "-----BEGIN ${dir}${name} gzip base64 ${size} bytes sha256 ${sum}-----"
            base64 -w 4000 "$packed"
            echo "-----END ${dir}${name}-----"
        fi
        rm -f "$packed"
    done
done
