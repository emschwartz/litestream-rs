#!/bin/sh
# A throwaway MinIO for the S3 conformance suite (just test-s3). Never real
# credentials: the root user and password below exist only inside the
# container this script starts and are also the test suite's defaults.
#
#   minio.sh up       start the container and create the bucket (fails when Docker is down,
#                     unless LSR_SKIP names minio)
#   minio.sh running  exit 0 when the container is up
#   minio.sh down     remove the container
#   minio.sh env      print the export lines the test recipes eval (endpoint, bucket, credentials)
#
# The image is Chainguard's MinIO build, pinned by digest: MinIO no longer
# publishes images itself. If the digest stops resolving, re-pin it and re-run
# the MinIO spike questions in docs/spikes.md.
set -eu

IMAGE=cgr.dev/chainguard/minio@sha256:e9cf6b99c226e9539ed0f14b6f00e37b07da153c7ba1f4f8709c807574b471ec
NAME=${LSR_MINIO_NAME:-lsr-minio-test}
PORT=${LSR_MINIO_PORT:-9000}
ROOT_USER=lsrtestroot
ROOT_PASSWORD=lsrtest-throwaway-0000
BUCKET=lsr-test
ENDPOINT="http://127.0.0.1:$PORT"

have_docker() {
    docker info >/dev/null 2>&1
}

running() {
    [ "$(docker inspect -f '{{.State.Running}}' "$NAME" 2>/dev/null || true)" = true ]
}

wait_ready() {
    tries=0
    until curl -fsS -o /dev/null "$ENDPOINT/minio/health/ready" 2>/dev/null; do
        tries=$((tries + 1))
        if [ "$tries" -gt 120 ]; then
            echo "minio: not ready after 60 s" >&2
            exit 1
        fi
        sleep 0.5
    done
}

make_bucket() {
    code=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT \
        --aws-sigv4 "aws:amz:us-east-1:s3" --user "$ROOT_USER:$ROOT_PASSWORD" \
        "$ENDPOINT/$BUCKET")
    # 409: the bucket is already there from an earlier run.
    case "$code" in
        200 | 409) ;;
        *) echo "minio: creating bucket failed (status $code)" >&2; exit 1 ;;
    esac
}

case "${1:-}" in
    up)
        if ! have_docker; then
            case ",${LSR_SKIP:-}," in
                *,minio,*)
                    if [ -n "${CI:-}" ]; then
                        echo "minio: LSR_SKIP is set under CI; CI never skips" >&2
                        exit 1
                    fi
                    echo "minio: Docker is not running; skipped by LSR_SKIP=minio"
                    exit 0
                    ;;
            esac
            echo "minio: Docker is not running (opt out on purpose with LSR_SKIP=minio)" >&2
            exit 1
        fi
        if ! running; then
            docker rm -f "$NAME" >/dev/null 2>&1 || true
            docker run -d --rm --name "$NAME" -p "127.0.0.1:$PORT:9000" \
                -e MINIO_ROOT_USER="$ROOT_USER" -e MINIO_ROOT_PASSWORD="$ROOT_PASSWORD" \
                "$IMAGE" server /tmp/data >/dev/null
        fi
        wait_ready
        make_bucket
        echo "minio: ready at $ENDPOINT, bucket $BUCKET"
        ;;
    running)
        have_docker && running
        ;;
    env)
        printf 'export LSR_MINIO_ENDPOINT=%s\n' "$ENDPOINT"
        printf 'export LSR_COMPAT_S3_ENDPOINT=%s\n' "$ENDPOINT"
        printf 'export LSR_COMPAT_S3_BUCKET=%s\n' "$BUCKET"
        printf 'export LSR_COMPAT_S3_ACCESS_KEY=%s\n' "$ROOT_USER"
        printf 'export LSR_COMPAT_S3_SECRET_KEY=%s\n' "$ROOT_PASSWORD"
        ;;
    down)
        if have_docker; then
            docker rm -f "$NAME" >/dev/null 2>&1 || true
        fi
        ;;
    *)
        echo "usage: minio.sh up|running|env|down" >&2
        exit 2
        ;;
esac