#!/usr/bin/env bash
# Runs one command against a real provider's bucket (docs/spikes.md, docs/release-bar.md R9):
# `scripts/provider.sh <tigris|aws|r2> <command...>`. Credentials come from a file outside the
# repo (~/.config/litestream-rs/<provider>.env, or LSR_PROVIDER_ENV), else, for Tigris, the AWS_*
# variables already set (a cloud session's environment carries them that way). One fresh run
# prefix goes in LSR_PROVIDER_PREFIX, and everything the command writes sits under it. When the
# command ends, this script deletes the prefix's objects and aborts its multipart uploads with
# the aws CLI, lists both again, and fails if either is not empty.
set -euo pipefail
provider="${1:?usage: provider.sh <tigris|aws|r2> <command...>}"
shift
case "$provider" in
tigris) file=tigris.env ;;
aws) file=aws.env ;;
r2) file=r2.env ;;
*)
    echo "provider: '$provider' is not one of tigris, aws, r2" >&2
    exit 64
    ;;
esac
env_file="${LSR_PROVIDER_ENV:-$HOME/.config/litestream-rs/$file}"
if [ "$provider" = tigris ] && [ -n "${LSR_TIGRIS_ENV:-}" ]; then
    env_file="$LSR_TIGRIS_ENV"
fi
if [ -f "$env_file" ]; then
    set -a
    . "$env_file"
    set +a
elif [ "$provider" != tigris ] || [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ] || [ -z "${AWS_ENDPOINT_URL_S3:-}" ]; then
    echo "provider: no $env_file and no AWS_* credentials set for $provider; nothing run" >&2
    exit 1
fi
if [ "$provider" = aws ]; then
    bucket="${LSR_AWS_BUCKET:?aws.env sets LSR_AWS_BUCKET}"
    endpoint="https://s3.${AWS_REGION:?aws.env sets AWS_REGION}.amazonaws.com"
    endpoint_args=()
else
    bucket="${LSR_PROVIDER_BUCKET:-litestream-rs-test}"
    [ "$provider" = tigris ] && bucket="${LSR_TIGRIS_BUCKET:-$bucket}"
    endpoint="${AWS_ENDPOINT_URL_S3:?AWS_ENDPOINT_URL_S3 is not set}"
    endpoint_args=(--endpoint-url "$endpoint")
fi
prefix="provider-runs/$provider/$(date -u +%F)-$(openssl rand -hex 4)"
export LSR_PROVIDER="$provider" LSR_PROVIDER_ENDPOINT="$endpoint" \
    LSR_PROVIDER_BUCKET="$bucket" LSR_PROVIDER_PREFIX="$prefix"
if [ "$provider" = tigris ]; then
    # The Tigris-only tests (crates/cli/tests/cli/tigris.rs, soak_tigris.rs) read these.
    export LSR_TIGRIS_BUCKET="$bucket" LSR_TIGRIS_PREFIX="$prefix"
fi
# The AWS bucket's name carries the account id, so it is never printed.
shown_bucket="$bucket"
[ "$provider" = aws ] && shown_bucket='$LSR_AWS_BUCKET'
echo "provider: $provider bucket $shown_bucket prefix $prefix" >&2

# The aws CLI's newer default checksums are not accepted by every provider's DeleteObjects.
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
s3api() { aws "${endpoint_args[@]}" s3api "$@"; }
leftovers() { # objects or uploads still under the prefix, one per line
    s3api list-objects-v2 --bucket "$bucket" --prefix "$prefix/" --query 'Contents[].Key' --output text
    s3api list-multipart-uploads --bucket "$bucket" --prefix "$prefix/" --query 'Uploads[].Key' --output text
}
cleanup() {
    command -v aws >/dev/null || { echo "provider: the aws CLI is missing; the prefix was not cleaned" >&2; return 1; }
    local deleted
    deleted=$(aws "${endpoint_args[@]}" s3 rm "s3://$bucket/$prefix/" --recursive 2>&1 | grep -c '^delete:' || true)
    local aborted=0 key id
    while read -r key id; do
        [ -n "$key" ] && [ "$key" != None ] || continue
        s3api abort-multipart-upload --bucket "$bucket" --key "$key" --upload-id "$id" >/dev/null && aborted=$((aborted + 1))
    done < <(s3api list-multipart-uploads --bucket "$bucket" --prefix "$prefix/" --query 'Uploads[].[Key,UploadId]' --output text)
    local left
    left=$(leftovers | grep -v '^None$' | grep -v '^$' || true)
    echo "provider: cleanup deleted $deleted objects and aborted $aborted uploads" >&2
    if [ -n "$left" ]; then
        echo "provider: the prefix is NOT empty: $(echo "$left" | wc -l | tr -d ' ') entries left" >&2
        return 1
    fi
    echo "provider: prefix lists empty, objects and multipart uploads" >&2
}
status=0
"$@" || status=$?
cleanup || [ "$status" -ne 0 ] || status=1
exit $status
