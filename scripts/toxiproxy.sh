#!/bin/sh
# Toxiproxy between the engine and MinIO, for the network-fault tests (just test-s3).
#
#   toxiproxy.sh up       build the pinned server once (Go), then start it on loopback
#                         (fails when Go is missing, unless LSR_SKIP names toxiproxy)
#   toxiproxy.sh running  exit 0 when the server answers
#   toxiproxy.sh down     stop it
#   toxiproxy.sh env      print the export line the test recipes eval (the API address)
#
# Toxiproxy (github.com/Shopify/toxiproxy, MIT licence) is a TCP proxy whose faults ("toxics")
# are set over an HTTP API: a reset after some bytes, a timeout after the request was sent,
# slicing, bandwidth limits. The tests make their own proxies through the API, so the server
# needs no configuration. It is built from source with `go install` at a pinned version, as the
# Go Litestream builds are, into a cache outside the repository; nothing of it is committed and
# it is never linked into a shipped binary. The listen ports are loopback only.
set -eu

VERSION=v2.12.0
CACHE=${LSR_TOXIPROXY_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/lsr-toxiproxy}
BIN="$CACHE/$VERSION/toxiproxy-server"
PORT=${LSR_TOXIPROXY_PORT:-8474}
API="http://127.0.0.1:$PORT"
PIDFILE="$CACHE/server.pid"
LOG="$CACHE/server.log"

answers() {
    curl -fsS -o /dev/null "$API/version" 2>/dev/null
}

skip_or_fail() {
    case ",${LSR_SKIP:-}," in
        *,toxiproxy,*)
            if [ -n "${CI:-}" ]; then
                echo "toxiproxy: LSR_SKIP is set under CI; CI never skips" >&2
                exit 1
            fi
            echo "toxiproxy: $1; skipped by LSR_SKIP=toxiproxy"
            exit 0
            ;;
    esac
    echo "toxiproxy: $1 (opt out on purpose with LSR_SKIP=toxiproxy)" >&2
    exit 1
}

build() {
    [ -x "$BIN" ] && return 0
    command -v go >/dev/null 2>&1 || skip_or_fail "Go is not installed, so the server cannot be built"
    mkdir -p "$CACHE/$VERSION"
    GOBIN="$CACHE/$VERSION" go install "github.com/Shopify/toxiproxy/v2/cmd/server@$VERSION"
    mv "$CACHE/$VERSION/server" "$BIN"
}

case "${1:-}" in
    up)
        build
        if ! answers; then
            mkdir -p "$CACHE"
            nohup "$BIN" -host 127.0.0.1 -port "$PORT" >"$LOG" 2>&1 &
            echo $! >"$PIDFILE"
            tries=0
            until answers; do
                tries=$((tries + 1))
                if [ "$tries" -gt 60 ]; then
                    echo "toxiproxy: not answering after 30 s; see $LOG" >&2
                    exit 1
                fi
                sleep 0.5
            done
        fi
        echo "toxiproxy: ready at $API"
        ;;
    running)
        answers
        ;;
    env)
        printf 'export LSR_TOXIPROXY_API=%s\n' "$API"
        ;;
    down)
        if [ -f "$PIDFILE" ]; then
            kill "$(cat "$PIDFILE")" 2>/dev/null || true
            rm -f "$PIDFILE"
        fi
        ;;
    *)
        echo "usage: toxiproxy.sh up|running|env|down" >&2
        exit 2
        ;;
esac
