#!/bin/sh
# A throwaway MinIO for the S3 conformance suite (just test-s3). Never real
# credentials: the root user and password below exist only inside the
# container this script starts and are also the test suite's defaults.
#
#   minio.sh up       start the container and create the bucket (fails when Docker is down,
#                     unless LSR_SKIP names minio)
#   minio.sh running  exit 0 when the container is up
#   minio.sh policy-user  create the user whose policy is only what Litestream's S3 guide publishes
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
# The MinIO client, pinned by digest, run inside the server container's network so it reaches
# the server on its own loopback; no mc on the host is needed.
MC_IMAGE=cgr.dev/chainguard/minio-client@sha256:11aa4baa385a6de421008f72d5019805475f2aa7d74d65d18b92c54986f36016
# A user whose policy is exactly the five actions Litestream's S3 guide publishes: the
# permissions a drop-in replacement must work with. Throwaway, like the root user.
POLICY_USER=lsrgopolicy
POLICY_PASSWORD=lsrgopolicy-throwaway-0000
POLICY_NAME=lsr-go-guide

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

# The user and policy of Litestream's S3 guide (GetBucketLocation and ListBucket on the bucket;
# PutObject, DeleteObject and GetObject on its objects): none of them allows listing or
# aborting multipart uploads. Idempotent.
make_policy_user() {
    policy=$(cat <<EOF
{"Version":"2012-10-17","Statement":[
{"Effect":"Allow","Action":["s3:GetBucketLocation","s3:ListBucket"],"Resource":["arn:aws:s3:::$BUCKET"]},
{"Effect":"Allow","Action":["s3:PutObject","s3:DeleteObject","s3:GetObject"],"Resource":["arn:aws:s3:::$BUCKET/*"]}
]}
EOF
)
    mc() {
        docker run --rm -i --network "container:$NAME" \
            -e "MC_HOST_local=http://$ROOT_USER:$ROOT_PASSWORD@127.0.0.1:9000" \
            --entrypoint mc "$MC_IMAGE" "$@"
    }
    printf '%s' "$policy" | mc admin policy create local "$POLICY_NAME" /dev/stdin >/dev/null
    mc admin user add local "$POLICY_USER" "$POLICY_PASSWORD" >/dev/null
    mc admin policy attach local "$POLICY_NAME" --user "$POLICY_USER" >/dev/null 2>&1 || true
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
    policy-user)
        make_policy_user
        echo "minio: user $POLICY_USER holds only the actions of Litestream's S3 guide"
        ;;
    env)
        printf 'export LSR_MINIO_ENDPOINT=%s\n' "$ENDPOINT"
        printf 'export LSR_COMPAT_S3_ENDPOINT=%s\n' "$ENDPOINT"
        printf 'export LSR_COMPAT_S3_BUCKET=%s\n' "$BUCKET"
        printf 'export LSR_COMPAT_S3_ACCESS_KEY=%s\n' "$ROOT_USER"
        printf 'export LSR_COMPAT_S3_SECRET_KEY=%s\n' "$ROOT_PASSWORD"
        printf 'export LSR_COMPAT_S3_POLICY_ACCESS_KEY=%s\n' "$POLICY_USER"
        printf 'export LSR_COMPAT_S3_POLICY_SECRET_KEY=%s\n' "$POLICY_PASSWORD"
        ;;
    down)
        if have_docker; then
            docker rm -f "$NAME" >/dev/null 2>&1 || true
        fi
        ;;
    *)
        echo "usage: minio.sh up|running|policy-user|env|down" >&2
        exit 2
        ;;
esac