#!/usr/bin/env bash
# Runs one command against the real Tigris bucket (docs/spikes.md):
# credentials from a file outside the repo, else the AWS_* variables already set
# (a cloud session's environment carries them that way), and one fresh run prefix
# in LSR_TIGRIS_PREFIX that everything the command writes sits under.
set -euo pipefail
env_file="${LSR_TIGRIS_ENV:-$HOME/.config/litestream-rs/tigris.env}"
if [ -f "$env_file" ]; then
    set -a
    . "$env_file"
    set +a
elif [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ] || [ -z "${AWS_ENDPOINT_URL_S3:-}" ]; then
    echo "tigris: no $env_file and no AWS_* credentials set; nothing run" >&2
    exit 1
fi
export LSR_TIGRIS_BUCKET="${LSR_TIGRIS_BUCKET:-litestream-rs-test}"
export LSR_TIGRIS_PREFIX="tigris-runs/$(date -u +%F)-$(openssl rand -hex 4)"
echo "tigris: bucket $LSR_TIGRIS_BUCKET prefix $LSR_TIGRIS_PREFIX" >&2
status=0
"$@" || status=$?
echo "tigris: check that $LSR_TIGRIS_BUCKET/$LSR_TIGRIS_PREFIX/ lists empty (objects and multipart uploads) with the aws CLI (see docs/spikes.md)" >&2
exit $status
