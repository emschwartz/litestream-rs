# Every recipe is scoped so it can run inside a worktree.
default: check

# The land gate: format, lints, tests. A branch lands only when this is green.
check: fmt-check clippy test doctest features ledger-fresh

fmt-check:
    cargo fmt --all -- --check

# Advisories, bans, licenses and sources over every feature (deny.toml); needs cargo-deny.
deny:
    cargo deny --locked check

clippy:
    cargo clippy --workspace --all-targets --locked -- -D warnings

test:
    cargo nextest run --workspace --locked --no-tests=warn

# Commit subjects on this branch since `base` follow Conventional Commits (AGENTS.md).
# The default base is local `main`, which `just spawn` branches from; without one, `origin/main`.
commit-check base="":
    #!/usr/bin/env bash
    set -euo pipefail
    base="{{base}}"
    if [ -z "$base" ]; then
        if git rev-parse --verify --quiet main >/dev/null; then base=main; else base=origin/main; fi
    fi
    pattern='^(feat|fix|perf|refactor|test|docs|build|ci|chore|revert)(\([a-z0-9-]+\))?!?: [^A-Z].{0,70}[^.]$'
    bad=0
    while IFS= read -r line; do
        subject="${line#* }"
        case "$subject" in Merge\ *) continue ;; esac
        if ! printf '%s' "$subject" | grep -Eq "$pattern"; then
            echo "commit-check: ${line%% *}: $subject" >&2
            bad=1
        fi
    done < <(git log --format='%h %s' "$base..HEAD")
    if [ "$bad" -ne 0 ]; then
        echo "commit-check: subjects must be 'type(scope): summary' (see AGENTS.md)" >&2
        exit 1
    fi

# The S3 store's HTTP clients (docs/plans/phase-1-reading.md, ruling F2 as amended):
# each builds alone, all build together, and reqwest's own tests run (ureq's run in `test`),
# then the test of a host's native-tls client, which needs the `native-tls` feature.
features:
    cargo clippy -p lsr-store-s3 --no-default-features --lib --locked -- -D warnings
    cargo clippy -p lsr-store-s3 --no-default-features --features reqwest --lib --locked -- -D warnings
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
    cargo nextest run -p lsr-store-s3 --features reqwest --lib --locked
    cargo nextest run -p lsr-store-s3 --features native-tls --lib --locked native_tls

# nextest does not run doctests, and the compile-fail proofs (a proof token is not Clone) live there.
doctest:
    cargo test --doc --workspace --all-features --locked

# Replays one simulator run, printing every operation: just sim-replay 0x2a [steps].
sim-replay seed steps="300":
    LSR_SIM_SEED={{seed}} LSR_SIM_STEPS={{steps}} cargo nextest run -p lsr-sim --test sim -E 'test(/^replay::/)' --no-capture --locked

# Runs rotating simulator seeds for the given seconds from a base seed (default: now).
sim-soak seconds base=`date +%s`:
    LSR_SIM_BASE_SEED={{base}} LSR_SIM_SECONDS={{seconds}} cargo nextest run -p lsr-sim --test sim -E 'test(/^soak::/)' --run-ignored only --no-capture --locked

# The nightly rotating seed set: seeds follow from the UTC date (YYYYMMDD), a million per night,
# so no two nights share one and a night is rerun with `just sim-nightly <seconds> <YYYYMMDD>`.
# A failing seed is printed last as its replay line. The arguments reach the test as environment
# variables only, never as shell text.
sim-nightly $LSR_SIM_SECONDS="900" $LSR_SIM_DAY=`date -u +%Y%m%d`:
    env -u LSR_SIM_BASE_SEED cargo nextest run -p lsr-sim --test sim -E 'test(/^soak::/)' --run-ignored only --no-capture --locked

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

# The large real-SQLite cases (docs/plans/phase-3-syncing.md, the fault suite): a 16 KiB-page
# database past 1 GiB, one past 4 GiB (skipped below 20 GB free disk), a WAL past the sync cap.
# Minutes and several gigabytes of temporary files; not part of check.
sqlite-big:
    cargo nextest run -p lsr-sqlite --test sqlite -E 'test(/^big::/)' --run-ignored only --no-capture --locked

# Build the pinned Go binaries and their VFS extensions, then run the live checks against them
# (not part of check; the VFS test also needs a C compiler and the SQLite headers).
# The VFS test's C host needs a SQLite with sqlite3_load_extension. It takes SQLITE3_CFLAGS and
# SQLITE3_LIBS when set, then on macOS Homebrew's (`brew install sqlite`; the system one lacks
# the function), then `pkg-config sqlite3`, then plain -lsqlite3 (crates/compat/src/vfs.rs).
# The ledger's counterpart check is ignored too, but it needs the listing ledger-fresh writes.
compat:
    ./scripts/build-go-litestream.sh
    cargo nextest run -p lsr-compat --run-ignored all --locked -E 'not test(=ledger::every_counterpart_exists)'

# Build the named Go versions (tags, commits, `releases` for every v0.5.N tag, `main` for
# upstream's tip) and run the live tests over exactly those: just compat-live v0.5.3 main.
# A live test is one in a `mod live` (lsr_compat::LIVE_MODULE); asking for the builds from
# anywhere else fails the test, so one moved out of the filter cannot drop out unnoticed.
compat-live +names:
    ./scripts/build-go-litestream.sh {{names}}
    LSR_COMPAT_VERSIONS="{{names}}" cargo nextest run -p lsr-compat --run-ignored all --locked --no-fail-fast -E 'test(/(^|::)live::/)'

# Every release tag v0.5.N: Go writes and Rust restores, Rust writes and Go restores, handover both
# ways, and (from v0.5.16, the first release that reads Rust's files) Go and Rust alternating on one prefix.
compat-releases: (compat-live "releases")

# Every upstream release tag is in the compat table (fetches the clone's tags first).
compat-tags:
    ./scripts/fetch-upstream.sh
    cargo nextest run -p lsr-compat --run-ignored all --locked -E 'test(every_release_tag_is_known)'

# Regenerate the Go compat fixtures (all, or one: just compat-regen --version v0.5.18 --scenario small-4096).
compat-regen *args:
    ./scripts/build-go-litestream.sh
    cargo run --locked -p lsr-compat --bin compat-regen -- {{args}}

# Run one fuzz target (decode, index, plan, key, wal) for N seconds; new inputs join fuzz/corpus.
fuzz target seconds:
    cargo +nightly fuzz run {{target}} --fuzz-dir fuzz -- -max_total_time={{seconds}} -max_len=32768 -rss_limit_mb=2048 -print_final_stats=1

# Minimise one target's corpus (keeps what reaches distinct coverage); extra
# arguments go to libFuzzer: just fuzz-cmin wal -max_len=16384.
fuzz-cmin target *args:
    cargo +nightly fuzz cmin {{target}} --fuzz-dir fuzz -- {{args}}

# Rebuild the seed corpus from the Go-written fixtures, and the wal seeds from built and SQLite WALs.
fuzz-seed:
    cargo nextest run -p lsr-fuzzing --test fuzzing -E 'test(/^seed::/)' --run-ignored only --locked

# A throwaway MinIO in Docker for the S3 conformance suite (skips when Docker is down).
minio-up:
    ./scripts/minio.sh up

minio-down:
    ./scripts/minio.sh down

# The S3 store against a real server; not part of check, which never needs Docker.
# Locally it skips (exit 0) when Docker is down. Under CI (the CI variable, which GitHub sets) or
# with LSR_REQUIRE_MINIO set, the tests are told a server is required: they fail, never skip,
# when MinIO is missing or unreachable, so a green run means they ran against it.
test-s3: minio-up
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "${CI:-}" ] && [ -z "${LSR_REQUIRE_MINIO:-}" ]; then
        export LSR_REQUIRE_MINIO=1
    fi
    if [ -z "${LSR_REQUIRE_MINIO:-}" ] && ! ./scripts/minio.sh running; then
        echo "test-s3: MinIO is not running; S3 tests skipped"
        exit 0
    fi
    LSR_MINIO_ENDPOINT="http://127.0.0.1:${LSR_MINIO_PORT:-9000}" \
        cargo nextest run -p lsr-store-s3 --test store-s3 -E 'test(/^minio::/)' --locked --no-fail-fast
    LSR_MINIO_ENDPOINT="http://127.0.0.1:${LSR_MINIO_PORT:-9000}" LSR_S3_TRANSPORT=reqwest \
        cargo nextest run -p lsr-store-s3 --features reqwest --test store-s3 -E 'test(/^minio::/)' --locked --no-fail-fast
    LSR_MINIO_ENDPOINT="http://127.0.0.1:${LSR_MINIO_PORT:-9000}" \
        cargo nextest run -p lsr-cli --test cli -E 'test(/^s3::/)' --locked --no-fail-fast

# Each restores what the other wrote at Latest, a txid and an instant, and they hand one prefix
# over both ways, over the pinned builds (crates/compat/tests/compat/s3.rs). Needs Docker; fails, never
# skips, when MinIO is not up. Never a real store: every AWS_* and LITESTREAM_* variable is unset
# first, the tests read only the LSR_COMPAT_S3_* ones set here (MinIO's throwaway credentials
# from scripts/minio.sh) and refuse any endpoint but loopback http.
# Go and Rust on one MinIO bucket: restores and handovers both ways.
compat-s3: minio-up
    #!/usr/bin/env bash
    set -euo pipefail
    if ! ./scripts/minio.sh running; then
        echo "compat-s3: MinIO is not running (it needs Docker); nothing ran" >&2
        exit 1
    fi
    for var in $(compgen -e); do
        case "$var" in AWS_* | LITESTREAM_*) unset "$var" ;; esac
    done
    ./scripts/build-go-litestream.sh
    LSR_COMPAT_S3_ENDPOINT="http://127.0.0.1:${LSR_MINIO_PORT:-9000}" \
        LSR_COMPAT_S3_BUCKET=lsr-test \
        LSR_COMPAT_S3_ACCESS_KEY=lsrtestroot \
        LSR_COMPAT_S3_SECRET_KEY=lsrtest-throwaway-0000 \
        cargo nextest run -p lsr-compat --test compat -E 'test(/^s3::/)' --run-ignored all --locked --no-fail-fast

# The two alternation runs (Go and Rust taking turns on one prefix, each turn killed at a
# seeded point) over the pinned builds; compat-releases.yml prints the seed of a nightly run.
# Both alternation runs with one seed: just compat-alternation <seed> replays a nightly failure.
compat-alternation $LSR_ALTERNATION_SEED:
    ./scripts/build-go-litestream.sh
    cargo nextest run -p lsr-compat --test compat --run-ignored all --locked --no-fail-fast --no-capture -E 'test(go_and_rust_alternate_on_one_prefix) | test(go_and_rust_sidecars_alternate_on_one_config_file)'

# The store and the conformance suite against the real Tigris bucket.
# Never part of check or CI: it needs credentials (a file outside the repo, or
# AWS_* variables) and spends real requests (docs/spikes.md).
# Everything it writes sits under one run prefix, deleted afterwards with the aws CLI.
test-tigris:
    # LSR_S3_TRANSPORT=reqwest runs the same tests over the reqwest client.
    ./scripts/tigris.sh cargo nextest run -p lsr-store-s3 --features reqwest --test store-s3 -E 'test(/^tigris::/)' --run-ignored only --no-capture --locked --no-fail-fast

# A production-shaped restore: Go v0.5.18 replicates the compat workload straight to Tigris
# (a few hundred files, levels 0 to 3 and 9), then `lsr verify`, and `lsr restore`
# and Go's restore at the same targets compared on pages. A few minutes and a few
# thousand requests; the test deletes its run prefix, failed or not. Go's AWS SDK
# refuses AWS_CA_BUNDLE with litestream's own HTTP client, so it is unset; Go reads
# SSL_CERT_FILE for the same roots. Release build, so the elapsed times mean something.
# LSR_TIGRIS_PAGE_SIZE, LSR_TIGRIS_GROW_MIB and LSR_TIGRIS_MIN_FILES make the run larger
# (crates/cli/tests/cli/tigris.rs); unset, it is the few-minute run.
tigris-restore:
    ./scripts/build-go-litestream.sh v0.5.18
    env -u AWS_CA_BUNDLE ./scripts/tigris.sh cargo nextest run --release -p lsr-cli --test cli -E 'test(/^tigris::/)' --run-ignored only --no-capture --locked

# The long soak of `lsr replicate` against the Tigris bucket (docs/plans/phase-3-syncing.md, the soak section):
# a synthetic writer for the given hours, a restore at Latest and at a random sampled transaction
# compared with recorded page hashes every hour (LSR_SOAK_CHECK_SECS), resources and log warnings
# printed as HOUR lines. Never part of check or CI. Afterwards, check that the run prefix lists
# empty, multipart uploads included.
soak-tigris hours:
    LSR_SOAK_HOURS={{hours}} ./scripts/tigris.sh cargo nextest run --release -p lsr-cli --test cli -E 'test(/^soak_tigris::/)' --run-ignored only --no-capture --locked

# Pinned upstream sources, read-only reference for the port and input to the compat harness.
upstream:
    ./scripts/fetch-upstream.sh

# Every counterpart the upstream-test ledger (docs/ledger.toml) names is a Rust test that exists,
# so a renamed or deleted counterpart fails the gate. The Rust test list is generated here into
# target/ and never committed, so a branch that adds a test touches no shared list. Some tests
# are Linux-only and the ledger names some of them, so on another OS the step is skipped; CI, on
# Linux, still enforces it.
ledger-fresh:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "$(uname)" != Linux ]; then
        echo "ledger-fresh: skipped: the ledger names Linux-only tests"
        exit 0
    fi
    # Absolute, since nextest runs the test in the package's directory.
    mkdir -p "${CARGO_TARGET_DIR:-target}/ledger"
    list="$(cd "${CARGO_TARGET_DIR:-target}/ledger" && pwd)/rust-tests.txt"
    cargo nextest list --workspace --all-features --locked --run-ignored all --message-format oneline --color never > "$list"
    LSR_LEDGER_RUST_TESTS="$list" cargo nextest run -p lsr-compat --test compat --locked --run-ignored only -E 'test(=ledger::every_counterpart_exists)'

# Regenerate the upstream test names in docs/ledger/upstream-tests.txt from the pinned clones
# (`just upstream` first). The ledger test in `check` reads this file, never the clone.
ledger:
    ./scripts/upstream-tests.sh > docs/ledger/upstream-tests.txt

# The large exhaustive planner sweeps (crates/core/tests/core/plan_exhaustive.rs): every inventory of
# up to five files, and of four files over a wider range. Minutes; not part of check.
plan-exhaustive:
    cargo nextest run -p lsr-core --test core --run-ignored only --no-capture --locked -E 'test(/^plan_exhaustive::/)'
