# Every recipe is scoped so it can run inside a worktree.
default: check

# The land gate: format, lints, tests. A branch lands only when this is green.
check: fmt-check clippy test doctest

fmt-check:
    cargo fmt --all -- --check

clippy:
    cargo clippy --workspace --all-targets --locked -- -D warnings

test:
    cargo nextest run --workspace --locked --no-tests=warn

# nextest does not run doctests, and the compile-fail proofs (a proof token is not Clone) live there.
doctest:
    cargo test --doc --workspace --all-features --locked

# Replays one simulator run, printing every operation: just sim-replay 0x2a [steps].
sim-replay seed steps="300":
    LSR_SIM_SEED={{seed}} LSR_SIM_STEPS={{steps}} cargo nextest run -p lsr-sim --test replay --no-capture --locked

# Runs rotating simulator seeds for the given seconds from a base seed (default: now).
sim-soak seconds base=`date +%s`:
    LSR_SIM_BASE_SEED={{base}} LSR_SIM_SECONDS={{seconds}} cargo nextest run -p lsr-sim --test soak --run-ignored only --no-capture --locked

# The nightly rotating seed set: seeds follow from the UTC date (YYYYMMDD), a million per night,
# so no two nights share one and a night is rerun with `just sim-nightly <seconds> <YYYYMMDD>`.
# A failing seed is printed last as its replay line. The arguments reach the test as environment
# variables only, never as shell text.
sim-nightly $LSR_SIM_SECONDS="900" $LSR_SIM_DAY=`date -u +%Y%m%d`:
    env -u LSR_SIM_BASE_SEED cargo nextest run -p lsr-sim --test soak --run-ignored only --no-capture --locked

# Mutation testing for one crate (just mutants ltx); results in target/mutants/mutants.out.
# Survivors are killed by a test or recorded in docs/mutants.md. Not part of check.
# The tree is copied with its .git: a guard test in core lists the tracked files, and
# without it the unmutated baseline fails and nothing is tested.
mutants crate *args:
    mkdir -p target/mutants
    cargo mutants -p lsr-{{crate}} --test-tool nextest --jobs 4 --copy-vcs true --output target/mutants {{args}}

# A branch in its own worktree under the main checkout's .worktrees/, off main's tip.
spawn name:
    git -C "{{justfile_directory()}}" worktree add ".worktrees/{{name}}" -b "{{name}}" main

# Build the pinned Go binaries, then run the live checks against them (not part of check).
compat:
    ./scripts/build-go-litestream.sh
    cargo nextest run -p lsr-compat --run-ignored all --locked

# Build the named Go versions (tags, commits, `releases` for every v0.5.N tag, `main` for
# upstream's tip) and run the live tests over exactly those: just compat-live v0.5.3 main.
compat-live +names:
    ./scripts/build-go-litestream.sh {{names}}
    LSR_COMPAT_VERSIONS="{{names}}" cargo nextest run -p lsr-compat --run-ignored all --locked --no-fail-fast -E 'test(go_restores_rust_chain_) | test(go_compacts_and_prunes) | test(go_resumes_) | test(rust_resumes_from_go_meta) | test(rust_restores_what_go_wrote) | test(go_restores_what_rust_encoded)'

# Every release tag v0.5.N: Go writes and Rust restores, Rust writes and Go restores, handover both ways.
compat-releases: (compat-live "releases")

# Every upstream release tag is in the compat table (fetches the clone's tags first).
compat-tags:
    ./scripts/fetch-upstream.sh
    cargo nextest run -p lsr-compat --run-ignored all --locked -E 'test(every_release_tag_is_known)'

# Regenerate the Go compat fixtures (all, or one: just compat-regen --version v0.5.18 --scenario small-4096).
compat-regen *args:
    ./scripts/build-go-litestream.sh
    cargo run --locked -p lsr-compat --bin compat-regen -- {{args}}

# Run one fuzz target (decode, index, plan, key) for N seconds; new inputs join fuzz/corpus.
fuzz target seconds:
    cargo +nightly fuzz run {{target}} --fuzz-dir fuzz -- -max_total_time={{seconds}} -max_len=32768 -rss_limit_mb=2048 -print_final_stats=1

# Minimise one target's corpus (keeps what reaches distinct coverage).
fuzz-cmin target:
    cargo +nightly fuzz cmin {{target}} --fuzz-dir fuzz

# Rebuild the seed corpus from the Go-written fixtures.
fuzz-seed:
    cargo nextest run -p lsr-fuzzing --test seed --run-ignored only --locked

# A throwaway MinIO in Docker for the S3 conformance suite (skips when Docker is down).
minio-up:
    ./scripts/minio.sh up

minio-down:
    ./scripts/minio.sh down

# The S3 store against a real server; not part of check, which never needs Docker.
test-s3: minio-up
    #!/usr/bin/env bash
    set -euo pipefail
    if ./scripts/minio.sh running; then
        LSR_MINIO_ENDPOINT="http://127.0.0.1:${LSR_MINIO_PORT:-9000}" \
            cargo nextest run -p lsr-store-s3 --test minio --locked --no-fail-fast
        LSR_MINIO_ENDPOINT="http://127.0.0.1:${LSR_MINIO_PORT:-9000}" \
            cargo nextest run -p lsr-cli --test s3 --locked --no-fail-fast
    else
        echo "test-s3: MinIO is not running; S3 tests skipped"
    fi

# The store, the conformance suite and the compat restore against the real
# Tigris bucket of task 8t. Never part of check or CI: it needs credentials (a
# file outside the repo, or AWS_* variables) and spends real requests (docs/spikes.md, Task 8t).
# Everything it writes sits under one run prefix, which the last step deletes.
test-tigris:
    #!/usr/bin/env bash
    set -euo pipefail
    # Credentials: the file when it exists, else the AWS_* variables already set
    # (a cloud session's environment carries them that way).
    env_file="${LSR_TIGRIS_ENV:-$HOME/.config/litestream-rs/tigris.env}"
    if [ -f "$env_file" ]; then
        set -a
        . "$env_file"
        set +a
    elif [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ] || [ -z "${AWS_ENDPOINT_URL_S3:-}" ]; then
        echo "test-tigris: no $env_file and no AWS_* credentials set; nothing run" >&2
        exit 1
    fi
    export LSR_TIGRIS_BUCKET="${LSR_TIGRIS_BUCKET:-litestream-rs-test}"
    export LSR_TIGRIS_PREFIX="8t/$(date -u +%F)-$(openssl rand -hex 4)"
    echo "test-tigris: bucket $LSR_TIGRIS_BUCKET prefix $LSR_TIGRIS_PREFIX" >&2
    status=0
    cargo nextest run -p lsr-store-s3 --test tigris --run-ignored only --no-capture --locked --no-fail-fast || status=$?
    echo "test-tigris: delete $LSR_TIGRIS_BUCKET/$LSR_TIGRIS_PREFIX/ with the aws CLI when done (see docs/spikes.md)" >&2
    exit $status

# Pinned upstream sources, read-only reference for the port and input to the compat harness.
upstream:
    ./scripts/fetch-upstream.sh