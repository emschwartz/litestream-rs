# Every recipe is scoped so it can run inside a worktree.
default: check

# The land gate: format, lints, tests. A branch lands only when this is green.
check: fmt-check clippy test test-opt doctest features upstream-test-map-fresh evidence-fresh

fmt-check:
    cargo fmt --all -- --check

# Advisories, bans, licenses and sources over every feature (deny.toml); needs cargo-deny.
deny:
    cargo deny --locked check

# Every feature at once: the features are additive, so this lints a superset of the default build,
# and it shares its build with `test`, `doctest` and `upstream-test-map-fresh`. The one code path
# the superset hides, `cfg(not(feature = "ureq"))` in the S3 store, is linted by `features`.
clippy:
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

# Every test that is not ignored, over every feature: ureq's and reqwest's transport tests
# and the native-tls classification tests among them.
test:
    cargo nextest run --workspace --all-features --locked --no-tests=warn

# The engine and both of its drivers again, built optimized (the `sim` profile: opt-level 1, every
# assertion kept). The engine ships optimized and the simulator soaks optimized, while `test`
# builds unoptimized; an optimizer that changes what the engine does fails here
# (docs/decisions/D185.md).
test-opt:
    cargo nextest run --cargo-profile sim -p lsr-core -p lsr-sim -p lsr-replica --all-features --locked

# The same three crates built the way the binary ships, twice: under `release-asserts` (release
# plus debug assertions and overflow checks) with every feature, then under `release` itself.
# The second run names its features because a release build refuses `test-half-stack`.
# Nightly only (docs/decisions/D187.md).
test-release-opt:
    cargo nextest run --cargo-profile release-asserts -p lsr-core -p lsr-sim -p lsr-replica --all-features --locked
    cargo nextest run --release -p lsr-core -p lsr-sim -p lsr-replica --features lsr-core/testing,lsr-core/verify,lsr-replica/rusqlite,lsr-replica/testing --locked

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

# The S3 store's HTTP clients: each builds
# alone, which `clippy` over all features cannot show. Their tests run in `test`, which has ureq,
# reqwest and native-tls on at once. Then lsr-sqlite and lsr-replica without `bundled` and without
# `rusqlite`, as an application linking its own SQLite builds them. Last, every crate's tests build
# with that crate alone selected: a test that leans on a feature only another crate turns on
# compiles under `--workspace` and fails in a recipe that names one package (`test-s3` did).
features:
    cargo clippy -p lsr-store-s3 --no-default-features --lib --locked -- -D warnings
    cargo clippy -p lsr-store-s3 --no-default-features --features reqwest --lib --locked -- -D warnings
    cargo clippy -p lsr-sqlite -p lsr-replica --no-default-features --lib --locked -- -D warnings
    for dir in crates/*/; do cargo check -p "lsr-$(basename "$dir")" --tests --locked || exit 1; done

# nextest does not run doctests, and the compile-fail proofs (a proof token is not Clone) live there.
doctest:
    cargo test --doc --workspace --all-features --locked

# Replays one simulator run, printing every operation: just sim-replay 0x2a [steps]. Built with the
# soaks' `sim` profile, so a seed replays the history the nightly ran.
sim-replay seed steps="300":
    LSR_SIM_SEED={{seed}} LSR_SIM_STEPS={{steps}} cargo nextest run --cargo-profile sim -p lsr-sim --test sim -E 'test(/^replay::/)' --no-capture --locked

# Sums what random simulator histories reach over `seeds` seeds from `base`: one count per state.
sim-tally seeds="200" base="1":
    LSR_SIM_TALLY_SEEDS={{seeds}} LSR_SIM_BASE_SEED={{base}} cargo nextest run -p lsr-sim --test sim -E 'test(/^tally::/)' --run-ignored only --no-capture --locked

# Runs rotating simulator seeds for the given seconds from a base seed (default: now).
sim-soak seconds base=`date +%s`:
    LSR_SIM_BASE_SEED={{base}} LSR_SIM_SECONDS={{seconds}} cargo nextest run --cargo-profile sim -p lsr-sim --test sim -E 'test(/^soak::/)' --run-ignored only --no-capture --locked

# The nightly rotating seed set: seeds follow from the UTC date (YYYYMMDD), a million per night,
# so no two nights share one and a night is rerun with `just sim-nightly <seconds> <YYYYMMDD>`.
# A failing seed is printed last as its replay line. The arguments reach the test as environment
# variables only, never as shell text. The summed reach goes to target/sim-reach.json, and a
# row or fault class no seed reached fails the run (crates/sim/src/marks.rs).
sim-nightly $LSR_SIM_SECONDS="900" $LSR_SIM_DAY=`date -u +%Y%m%d`:
    env -u LSR_SIM_BASE_SEED LSR_SIM_COMMIT="$(git rev-parse HEAD)" LSR_SIM_REACH_OUT="${LSR_SIM_REACH_OUT:-$PWD/target/sim-reach.json}" cargo nextest run --cargo-profile sim -p lsr-sim --test sim -E 'test(/^soak::/)' --run-ignored only --no-capture --locked

# The nightly toolchain for the sanitizer, Miri and fuzz recipes; override with LSR_NIGHTLY=nightly-YYYY-MM-DD.
nightly := env("LSR_NIGHTLY", "nightly")

# AddressSanitizer over the crate with `unsafe` FFI (sqlite) and the replica thread tests.
# Nightly only (-Zsanitizer, -Zbuild-std so the standard library is instrumented too); its own
# target dir, since the flags rebuild everything. Bundled SQLite's C code is not instrumented,
# but its allocations go through the sanitizer's allocator. Not part of check.
asan:
    RUSTFLAGS="-Zsanitizer=address" RUSTDOCFLAGS="-Zsanitizer=address" CARGO_TARGET_DIR=target/asan \
        cargo +{{nightly}} test -Zbuild-std --target x86_64-unknown-linux-gnu --locked \
        -p lsr-sqlite -p lsr-replica --lib --tests

# ThreadSanitizer over the same crates. The standard library is rebuilt with it (-Zbuild-std),
# or its mutexes and channels would look like unsynchronised memory.
tsan:
    RUSTFLAGS="-Zsanitizer=thread" CARGO_TARGET_DIR=target/tsan \
        cargo +{{nightly}} test -Zbuild-std --target x86_64-unknown-linux-gnu --locked \
        -p lsr-sqlite -p lsr-replica --lib --tests

# Miri over the pure crates (ltx, wal), the core's decision code (plan, retry, position,
# layout, checkpoint, compact), and lsr-sqlite's tests that call no SQLite (the build check, result
# codes, offsets, and the io_methods and wal-index checks on synthetic tables): no I/O, no FFI. Miri runs about 1000 times slower than native,
# so the core's engine-sized suites (actor, maintain, recover, sync and the rest, 500 tests) are
# left to the native run, the few whole-database tests inside the chosen modules are ignored
# under `cfg(miri)`, and the proptest case count is cut. A libtest filter matches anywhere in a
# test's path (`checkpoint::` also matched actor::tests::own_checkpoint, five engine-sized tests),
# so `--skip actor::` keeps the actor suite out. Environment reaches the program only
# through -Zmiri-env-forward. Not part of check.
miri:
    MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-env-forward=PROPTEST_CASES -Zmiri-env-forward=PROPTEST_DISABLE_FAILURE_PERSISTENCE" \
        PROPTEST_CASES=4 PROPTEST_DISABLE_FAILURE_PERSISTENCE=true CARGO_TARGET_DIR=target/miri \
        cargo +{{nightly}} miri test --locked -p lsr-ltx -p lsr-wal --lib
    MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-env-forward=PROPTEST_CASES -Zmiri-env-forward=PROPTEST_DISABLE_FAILURE_PERSISTENCE" \
        PROPTEST_CASES=4 PROPTEST_DISABLE_FAILURE_PERSISTENCE=true CARGO_TARGET_DIR=target/miri \
        cargo +{{nightly}} miri test --locked -p lsr-core --lib -- \
        --skip actor:: plan:: retry:: position:: layout:: checkpoint:: compact::
    MIRIFLAGS="-Zmiri-strict-provenance" CARGO_TARGET_DIR=target/miri \
        cargo +{{nightly}} miri test --locked -p lsr-sqlite --lib -- \
        version::tests::every version::tests::another version::tests::the_refusal error::tests \
        checkpoint::tests::checkpoint_results pages::tests::page_offsets pages::tests::a_missing \
        shm::tests::missing_methods identity::tests

# Schedule exploration with shuttle: the replica thread's command handshake and the S3 batch's
# scoped threads, under many random interleavings. Shuttle is a dev-dependency behind
# `--cfg shuttle`, so no other build sees it, and it needs its own target dir and a full rebuild, so it is
# not part of check: CI runs it on pull requests that touch store-s3 or replica, and nightly for 500000 schedules.
shuttle iterations="200":
    RUSTFLAGS="--cfg shuttle" CARGO_TARGET_DIR=target/shuttle \
        cargo clippy -p lsr-store-s3 -p lsr-replica --all-targets --locked -- -D warnings
    SHUTTLE_ITERATIONS={{iterations}} RUSTFLAGS="--cfg shuttle" CARGO_TARGET_DIR=target/shuttle \
        cargo test --locked -p lsr-store-s3 --lib shuttle_tests
    SHUTTLE_ITERATIONS={{iterations}} RUSTFLAGS="--cfg shuttle" CARGO_TARGET_DIR=target/shuttle \
        cargo test --locked -p lsr-replica --test replica shuttle::

# Every planted-bug patch, and every benchmark arm's patch (crates/compat/bench-patches), still
# applies to the tree. Seconds; nightly in CI, not in check, so a
# moved line near a planted site does not block a land. Whether the planted defect is caught is
# `planted-bugs`'s question.
planted-bugs-apply:
    #!/usr/bin/env bash
    set -euo pipefail
    stale=()
    found=0
    for patch in docs/planted-bugs/*.patch crates/compat/bench-patches/*.patch; do
        found=$((found + 1))
        git apply --check "$patch" 2>/dev/null || stale+=("$patch")
    done
    if [ "$found" -eq 0 ]; then
        echo "planted-bugs-apply: no patches found under docs/planted-bugs" >&2
        exit 1
    fi
    if [ "${#stale[@]}" -gt 0 ]; then
        printf 'planted-bugs-apply: no longer applies (re-express it, see docs/planted-bugs.md): %s\n' "${stale[@]}" >&2
        exit 1
    fi
    echo "planted-bugs-apply: all $found patches apply"

# Every fixed defect is re-planted in a scratch tree and its named tests must fail
# (docs/planted-bugs.md). A survivor is a defect in the tests; a stale patch needs re-expressing.
# Nightly in CI, in eight shards (`--shard K/8`). `just planted-bugs --only listing-cursor` runs one entry.
planted-bugs *args:
    uv run scripts/planted-bugs.py {{args}}

# The swarm alone against each planted bug, no named test run: seeds to the first failure per
# entry (`docs/planted-bugs.md`). Reports; a plant no seed finds is a listed generator gap.
planted-bugs-unaided seeds="1000" *args:
    uv run scripts/planted-bugs.py --unaided --seeds {{seeds}} {{args}}

# Production lines per shipped crate (code, comment, blank; tests and the "testing" feature left
# out) and the change in code lines against `base`. Pull requests write it to the CI job summary.
prod-lines base="origin/main":
    uv run scripts/prod-lines.py --base {{base}}

# Mutation-tests the lines this branch changed in core and ltx against `base`; fails on a missed
# mutant not in docs/mutants-escapes.txt. Runs on pull requests in CI.
mutants-diff base="origin/main":
    scripts/mutants-diff.sh {{base}}

# Mutation testing for one crate (just mutants ltx); results in target/mutants/mutants.out.
# Survivors are killed by a test or recorded in docs/mutants.md. Not part of check.
# The tree is copied with its .git: a guard test in core lists the tracked files, and
# without it the unmutated baseline fails and nothing is tested.
mutants crate *args:
    mkdir -p target/mutants
    cargo mutants -p lsr-{{crate}} --test-tool nextest --jobs 4 --copy-vcs true --output target/mutants {{args}}

# A branch in its own worktree under the main checkout's .worktrees/, off main's newest staged file.
spawn name:
    git -C "{{justfile_directory()}}" worktree add ".worktrees/{{name}}" -b "{{name}}" main

# The lsr-sqlite suite on the SQLite one libsqlite3-sys version bundles, with the rusqlite
# release that pairs with it: `just sqlite-build 0.37.0 0.39.0`. CI runs it for every build the
# `version` module lists (crates/sqlite/src/version.rs). Cargo.lock is moved for the run and put
# back afterwards; the update needs the network.
sqlite-build $LSR_SQLITE_SYS $LSR_RUSQLITE:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target
    cp Cargo.lock target/sqlite-build.lock
    trap 'cp target/sqlite-build.lock Cargo.lock' EXIT
    cargo update -p rusqlite --precise "$LSR_RUSQLITE"
    cargo update -p libsqlite3-sys --precise "$LSR_SQLITE_SYS"
    cargo tree -p lsr-sqlite --all-features -i libsqlite3-sys --depth 0
    cargo nextest run -p lsr-sqlite --all-features --no-fail-fast

# The lsr-sqlite build against the system's SQLite in place of the bundled one
# (`LIBSQLITE3_SYS_USE_PKG_CONFIG`, which overrides `bundled`): the open accepts it exactly when
# it is a listed build, and when its version is in the range the whole suite runs on it.
sqlite-system:
    #!/usr/bin/env bash
    set -euo pipefail
    export LIBSQLITE3_SYS_USE_PKG_CONFIG=1 CARGO_TARGET_DIR=target/sqlite-system
    echo "system SQLite: $(pkg-config --modversion sqlite3)"
    cargo nextest run -p lsr-sqlite --all-features --locked --lib -E 'test(=version::tests::the_linked_build_is_accepted_exactly_when_it_is_listed)'
    if pkg-config --atleast-version=3.51.3 sqlite3; then
        cargo nextest run -p lsr-sqlite --all-features --locked --no-fail-fast
    else
        echo "sqlite-system: below 3.51.3, so only the refusal is checked"
    fi

# An application on sqlx's SQLite and lsr-replica in one build (`sqlx-check/`, outside the
# workspace). Its lock holds sqlx 0.9.0, which needs libsqlite3-sys below 0.38, so lsr-sqlite links
# 0.37, the oldest end of its range. Builds only; nothing runs.
sqlx-check:
    cargo build --manifest-path sqlx-check/Cargo.toml --locked
    cargo tree --manifest-path sqlx-check/Cargo.toml --locked -i libsqlite3-sys --depth 0

# The large real-SQLite cases: a 16 KiB-page
# database past 1 GiB, one past 4 GiB (skipped below 20 GB free disk), a WAL past the sync cap.
# Minutes and several gigabytes of temporary files; not part of check.
sqlite-big:
    cargo nextest run -p lsr-sqlite --test sqlite -E 'test(/^big::/)' --run-ignored only --no-capture --locked

# Build the pinned Go binaries and their VFS extensions, then run the live checks against them
# (not part of check; the VFS test also needs a C compiler and the SQLite headers).
# The VFS test's C application needs a SQLite with sqlite3_load_extension. It takes SQLITE3_CFLAGS and
# SQLITE3_LIBS when set, then on macOS Homebrew's (`brew install sqlite`; the system one lacks
# the function), then `pkg-config sqlite3`, then plain -lsqlite3 (crates/compat/src/vfs.rs).
# The test map's counterpart check and the behaviour citation check are ignored too, but they need
# the listings upstream-test-map-fresh and evidence-fresh write, and run there (both in check).
# The s3:: tests need MinIO, so they belong to compat-s3, which starts it; here they would fail closed.
compat:
    ./scripts/build-go-litestream.sh
    cargo nextest run -p lsr-compat --run-ignored all --locked -E 'not test(=upstream_test_map::every_counterpart_exists) and not test(=behavior::every_behavior_citation_resolves) and not test(/^s3::/)'

# Build the named Go versions (tags, commits, `releases` for every v0.5.N tag, `main` for
# upstream's newest staged file) and run the live tests over exactly those: just compat-live v0.5.3 main.
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

# The triple cargo-fuzz builds for: the nightly toolchain's own host. Left to itself cargo-fuzz
# picks the triple it was compiled for, so a musl build of it (what cargo-binstall fetches) asks
# for x86_64-unknown-linux-musl, where the address sanitizer cannot link and nightly has no std.
fuzz_triple := "$(rustc +nightly -vV | sed -n 's/^host: //p')"

# Run one fuzz target (decode, flip, index, plan, plan_contract, plan_lazy, key, wal) for N seconds;
# new inputs join fuzz/corpus.
fuzz target seconds:
    cargo +nightly fuzz run {{target}} --fuzz-dir fuzz --target "{{fuzz_triple}}" -- -max_total_time={{seconds}} -max_len=32768 -rss_limit_mb=2048 -print_final_stats=1

# Replay one input (a finding under fuzz/artifacts, or a corpus file) against its target.
fuzz-replay target file:
    cargo +nightly fuzz run {{target}} --fuzz-dir fuzz --target "{{fuzz_triple}}" "{{file}}"

# Minimise one target's corpus (keeps what reaches distinct coverage); extra
# arguments go to libFuzzer: just fuzz-cmin wal -max_len=16384.
fuzz-cmin target *args:
    cargo +nightly fuzz cmin {{target}} --fuzz-dir fuzz --target "{{fuzz_triple}}" -- {{args}}

# Rebuild the seed corpus from the Go-written fixtures, and the wal seeds from built and SQLite WALs.
fuzz-seed:
    cargo nextest run -p lsr-fuzzing --test fuzzing -E 'test(/^seed::/)' --run-ignored only --locked

# A throwaway MinIO in Docker for the S3 conformance suite. Fails when Docker is down, unless the
# runner opts out on purpose with LSR_SKIP=minio (never under CI).
minio-up:
    ./scripts/minio.sh up

minio-down:
    ./scripts/minio.sh down

# Toxiproxy on loopback, between the engine and MinIO in the network-fault tests.
toxiproxy-up:
    ./scripts/toxiproxy.sh up

toxiproxy-down:
    ./scripts/toxiproxy.sh down

# The S3 store against a real server; not part of check, which never needs Docker (these tests are
# #[ignore]d there). Without MinIO the tests fail, so a green run means they ran against it. The
# one opt-out is LSR_SKIP=minio (`LSR_SKIP=minio just test-s3` runs nothing and says so); under
# CI, which GitHub marks with the CI variable, any LSR_SKIP is itself a failure. The history
# judge's network tests also need Toxiproxy in front of MinIO (scripts/toxiproxy.sh builds the
# pinned server with Go once and starts it on loopback); LSR_SKIP=toxiproxy opts out of those.
test-s3: minio-up toxiproxy-up
    #!/usr/bin/env bash
    set -euo pipefail
    case ",${LSR_SKIP:-}," in
        *,minio,*)
            if [ -n "${CI:-}" ]; then
                echo "test-s3: LSR_SKIP is set under CI; CI never skips" >&2
                exit 1
            fi
            echo "test-s3: skipped by LSR_SKIP=minio; nothing ran"
            exit 0
            ;;
    esac
    eval "$(./scripts/minio.sh env)"
    cargo nextest run -p lsr-store-s3 --test store-s3 -E 'test(/^minio::/)' --run-ignored all --locked --no-fail-fast
    LSR_S3_TRANSPORT=reqwest \
        cargo nextest run -p lsr-store-s3 --features reqwest --test store-s3 -E 'test(/^minio::/)' --run-ignored all --locked --no-fail-fast
    cargo nextest run -p lsr-cli --test cli -E 'test(/^s3::/)' --run-ignored all --locked --no-fail-fast
    eval "$(./scripts/toxiproxy.sh env)"
    cargo nextest run -p lsr-history --test history -E 'test(/^replicator::/) | test(/^network::/)' --run-ignored all --locked --no-fail-fast

# Each restores what the other wrote at Latest, a txid and an instant, and they hand one prefix
# over both ways, over the pinned builds (crates/compat/tests/compat/s3.rs). Needs Docker; fails, never
# skips, when MinIO is not up (LSR_SKIP=minio opts out on purpose, outside CI). Never a real store: every AWS_* and LITESTREAM_* variable is unset
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
    ./scripts/minio.sh policy-user
    eval "$(./scripts/minio.sh env)"
    cargo nextest run -p lsr-compat --test compat -E 'test(/^s3::/)' --run-ignored all --locked --no-fail-fast

# Go Litestream against lsr on this machine and a local MinIO (issue 42): commit to durable, host
# overhead, restore. Exploratory numbers on whatever machine runs it. Sets: all, or any of
# aa-restore aa-latency aa-overhead latency-steady latency-bursty overhead restore. Needs Docker, the
# Go builds and perfstats.py (LSR_PERFSTATS, default the perf-experiment skill's). Never part of check.
bench-vs-go out *sets="all": minio-up
    #!/usr/bin/env bash
    set -euo pipefail
    for var in $(compgen -e); do
        case "$var" in AWS_* | LITESTREAM_*) unset "$var" ;; esac
    done
    ./scripts/build-go-litestream.sh 041a8c3
    cargo build --release --locked -p lsr-cli --bin lsr
    cargo build --release --locked -p lsr-compat --bin bench-vs-go
    eval "$(./scripts/minio.sh env)"
    export LSR_PERFSTATS="${LSR_PERFSTATS:-$HOME/.claude/skills/perf-experiment/perfstats.py}"
    CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}" "${CARGO_TARGET_DIR:-$PWD/target}/release/bench-vs-go" --out "{{out}}" {{sets}}

# What a full copy of a database larger than memory does to an application reading a hot set
# beside it, and which remedy brings it back (issue 315). Linux with 8 GiB of memory and about
# 80 GB of free disk under <out>; needs Docker, Rust, uv and root (to drop the page cache between
# runs). Builds lsr and one binary per patch in crates/compat/bench-patches (about 20 minutes), builds
# the 16 GiB database (about 15 minutes), then the calibration round, the A/A gate and the rounds:
# about 5 hours on local NVMe and 10 to 11 on a 125 MB/s network disk (the throttled copies
# run 2, 4 and 8 times as long as the plain one; --rounds N shortens it). The frame and the arms are
# in crates/compat/src/bench/plan.rs; extra arguments (--rounds N, --arms A,B,..) go to the harness
# and make the run a check of it. Never part of check.
bench-snapshot-impact out *args: minio-up
    #!/usr/bin/env bash
    set -euo pipefail
    for var in $(compgen -e); do
        case "$var" in AWS_* | LITESTREAM_*) unset "$var" ;; esac
    done
    ./scripts/build-bench-arms.sh "{{out}}/arms"
    cargo build --release --locked -p lsr-cli --bin lsr
    cargo build --release --locked -p lsr-compat --bin bench-vs-go
    eval "$(./scripts/minio.sh env)"
    perfstats="${LSR_PERFSTATS:-$HOME/.claude/skills/perf-experiment/perfstats.py}"
    if [ -f "$perfstats" ]; then export LSR_PERFSTATS="$perfstats"; fi
    "${CARGO_TARGET_DIR:-$PWD/target}/release/bench-vs-go" --out "{{out}}" snapshot-impact --patched "{{out}}/arms" {{args}}

# The two alternation runs (Go and Rust taking turns on one prefix, each turn killed at a
# seeded point) over the pinned builds; compat-releases.yml prints the seed of a nightly run.
# Both alternation runs with one seed: just compat-alternation <seed> replays a nightly failure.
compat-alternation $LSR_ALTERNATION_SEED:
    ./scripts/build-go-litestream.sh
    cargo nextest run -p lsr-compat --test compat --run-ignored all --locked --no-fail-fast --no-capture -E 'test(go_and_rust_alternate_on_one_prefix) | test(go_and_rust_sidecars_alternate_on_one_config_file)'

# The store, the conformance suite and the provider checks (create-only writes and their races,
# listing and read consistency, error classification, abandoned uploads, and one `lsr replicate`
# and restore) against one real provider's bucket: tigris, aws or r2. For Tigris it also runs the
# Tigris-only probes. Never part of check or CI: it needs credentials (a file outside the repo,
# see scripts/provider.sh) and spends real requests (docs/spikes.md, docs/release-bar.md R9).
# Everything it writes sits under one run prefix, deleted afterwards with the aws CLI, and the
# script fails if the prefix does not list empty (objects and multipart uploads).
test-provider provider:
    ./scripts/provider.sh {{provider}} just _provider-tests

[private]
_provider-tests:
    #!/usr/bin/env bash
    set -euo pipefail
    store_filter='test(/^provider::/)'
    [ "$LSR_PROVIDER" = tigris ] && store_filter="$store_filter | test(/^tigris::/)"
    # LSR_S3_TRANSPORT=reqwest runs the same tests over the reqwest client.
    status=0
    # The tests run in parallel (a serial run of the long ones takes an hour on R2); each prints
    # its output when it ends.
    output='--success-output immediate --failure-output immediate'
    cargo nextest run -p lsr-store-s3 --features reqwest --test store-s3 -E "$store_filter" --run-ignored only $output --locked --no-fail-fast || status=$?
    cargo nextest run -p lsr-cli --test cli -E 'test(/^provider::/)' --run-ignored only $output --locked --no-fail-fast || status=$?
    exit $status

# The same as `just test-provider tigris`.
test-tigris: (test-provider "tigris")

# The consistency probe against the Tigris bucket (crates/store-s3/tests/store-s3/common/real.rs):
# one client writes and deletes keys, a second lists and reads right after, and every stale answer
# is counted in an `OBS consistency` table. About six requests a round. Never part of check or CI,
# which hold no store credentials; `just test-s3` runs the same probe against MinIO at 20 rounds.
consistency-probe-tigris $LSR_PROBE_ROUNDS="10000":
    ./scripts/tigris.sh cargo nextest run -p lsr-store-s3 --features reqwest --test store-s3 -E 'test(/^provider::shared_cases::consistency_probe$/)' --run-ignored only --no-capture --locked

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

# The long soak of `lsr replicate` against the Tigris bucket:
# a synthetic writer for the given hours, a restore at Latest and at a random sampled transaction
# compared with recorded page hashes every hour (LSR_SOAK_CHECK_SECS), resources and log warnings
# printed as HOUR lines. Never part of check or CI. Afterwards, check that the run prefix lists
# empty, multipart uploads included.
soak-tigris hours:
    LSR_SOAK_HOURS={{hours}} ./scripts/tigris.sh cargo nextest run --release -p lsr-cli --test cli -E 'test(/^soak_tigris::/)' --run-ignored only --no-capture --locked

# Pinned upstream sources, read-only reference for the port and input to the compat harness.
upstream:
    ./scripts/fetch-upstream.sh

# Every counterpart the upstream test map (docs/upstream-test-map.toml) names is a Rust test that exists,
# so a renamed or deleted counterpart fails the gate. The Rust test list is generated here into
# target/ and never committed, so a branch that adds a test touches no shared list. It runs on
# every system: the test excuses the few Linux-only counterparts it names (`LINUX_ONLY`) where
# the listing cannot hold them, and nothing else.
upstream-test-map-fresh:
    #!/usr/bin/env bash
    set -euo pipefail
    # Absolute, since nextest runs the test in the package's directory.
    mkdir -p "${CARGO_TARGET_DIR:-target}/upstream-test-map"
    list="$(cd "${CARGO_TARGET_DIR:-target}/upstream-test-map" && pwd)/rust-tests.txt"
    cargo nextest list --workspace --all-features --locked --run-ignored all --message-format oneline --color never > "$list"
    LSR_UPSTREAM_TEST_MAP_RUST_TESTS="$list" cargo nextest run -p lsr-compat --test compat --locked --run-ignored only -E 'test(=upstream_test_map::every_counterpart_exists)'

# Renders docs/BEHAVIOR.md from docs/behavior.toml (the behaviour table's rows) and
# docs/behavior/template.md (the prose); `check` fails when the committed file differs.
behavior-doc:
    LSR_BEHAVIOR_WRITE=1 cargo nextest run -p lsr-compat --test compat --locked -E 'test(=behavior::the_behavior_doc_is_rendered_from_its_rows)'

# The count of rows in docs/behavior.toml by mark, which no committed file holds.
behavior-tally:
    @cargo nextest run -p lsr-compat --test compat --locked --run-ignored only --no-capture -E 'test(=behavior::print_the_tally_of_rows_by_mark)' 2>&1 | sed -n '/^\*\*Tally/,/^ *PASS/p' | grep -v PASS

# Every test docs/behavior.toml cites is a test or a simulator scenario, and every planted bug a
# row cites is backed by a cited test that is not ignored, since `planted-bugs` runs no ignored
# test (crates/compat/tests/compat/behavior.rs).
# The listings are generated into target/ and never committed, as for the upstream test map.
evidence-fresh:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "${CARGO_TARGET_DIR:-target}/evidence"
    dir="$(cd "${CARGO_TARGET_DIR:-target}/evidence" && pwd)"
    cargo nextest list --workspace --all-features --locked --run-ignored all --message-format oneline --color never > "$dir/all.txt"
    cargo nextest list --workspace --all-features --locked --run-ignored only --message-format oneline --color never > "$dir/ignored.txt"
    LSR_EVIDENCE_ALL_TESTS="$dir/all.txt" LSR_EVIDENCE_IGNORED_TESTS="$dir/ignored.txt" \
        cargo nextest run -p lsr-compat --test compat --locked --run-ignored only -E 'test(=behavior::every_behavior_citation_resolves)'

# Regenerate the upstream test names in docs/upstream-test-map/upstream-tests.txt from the pinned clones
# (`just upstream` first). The test map test in `check` reads this file, never the clone.
upstream-test-map:
    ./scripts/upstream-tests.sh > docs/upstream-test-map/upstream-tests.txt

# The large exhaustive planner sweeps (crates/core/tests/core/plan_exhaustive.rs): every inventory of
# up to five files, and of four files over a wider range, plus the wider as-of sweeps. A superset of
# the small sweeps in `check`. Minutes; run by the nightly workflow's `plan-exhaustive` job.
plan-exhaustive:
    cargo nextest run -p lsr-core --test core --run-ignored only --no-capture --locked -E 'test(/^plan_exhaustive::/)'

# The memory gates at scale (crates/core/tests/core/mem_big.rs): restore from 1 to 4 GiB of
# database and from a million-file inventory, in release. The same laws run at small sizes in
# `check` (crates/core/tests/core/mem.rs). Needs 5 GiB of temporary disk, one database at a time.
mem-big:
    cargo nextest run --release -p lsr-core --test core -E 'test(/^mem_big::/)' --run-ignored only --no-capture --locked

# The real-SQLite power-loss campaigns, each judged by restores against the workload's own record
# (`lsr-history`): the history rig's 50 seeds of 6 cycles (`rig::real_power_loss_trials`) and the
# crash-cycle rig's 100 seeds of 12 cycles (`crash_cycles::crash_cycle_campaign`), which also
# restores while the loss still holds. About two minutes on 4 cores. Nightly in CI.
power-loss-trials:
    LSR_HISTORY_SEEDS=50 cargo nextest run -p lsr-history --all-features --locked --run-ignored only -E 'test(/^rig::real_power_loss_trials$/)' --no-capture
    LSR_CRASH_SEEDS=100 cargo nextest run -p lsr-sqlite --all-features --locked --run-ignored only -E 'test(/^crash_cycles::crash_cycle_campaign$/)' --no-capture

# Linux with /dev/fuse, fusermount3 and FUSE allowed (root, or user_allow_other); builds LazyFS once
# into ~/.cache/lsr-lazyfs (cmake, g++, git, libfuse3-dev). Not in `check`: a cycle takes about 3 s.
# Exit 1 on any anomaly; the last line printed is a JSON summary.
# Power-loss cycles of the real `lsr replicate` on LazyFS, judged by restores (scripts/lazyfs.py).
lazyfs cycles="50" *args:
    cargo build --release --locked -p lsr-cli
    cargo build --release --locked -p lsr-history --features sqlite
    test -x "${LSR_LAZYFS_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/lsr-lazyfs}/lazyfs/lazyfs/build/lazyfs" || uv run scripts/lazyfs.py build
    uv run scripts/lazyfs.py run --cycles {{cycles}} {{args}}

# The real `lsr replicate` beside a real SQLite writer, its replica on MinIO behind Toxiproxy, under a
# seeded menu of faults (kill -9, partitions, slow and cut transfers, the pinned Go beside it; LazyFS
# power loss on Linux), judged by point-in-time restores (scripts/fault_rig.py). Needs Docker, Go
# (Toxiproxy and Go Litestream are built once). MinIO on another port and name keeps a run off a
# shared one: LSR_MINIO_PORT=9101 LSR_MINIO_NAME=lsr-minio-fault just fault-rig 50. Exit 1 on any
# anomaly, 2 when the rig failed or a required situation was never reached.
fault-rig cycles="50" *args: minio-up toxiproxy-up
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --release --locked -p lsr-cli
    cargo build --release --locked -p lsr-history --features sqlite
    ./scripts/build-go-litestream.sh 041a8c3
    if [ "$(uname)" = Linux ]; then
        test -x "${LSR_LAZYFS_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/lsr-lazyfs}/lazyfs/lazyfs/build/lazyfs" || uv run scripts/lazyfs.py build
    fi
    eval "$(./scripts/minio.sh env)"
    eval "$(./scripts/toxiproxy.sh env)"
    # On Linux every nemesis of the menu must be able to run (LazyFS built, libfaketime installed):
    # a run that could not exercise one fails before the first cycle instead of printing a note.
    require=""
    if [ "$(uname)" = Linux ]; then require="--require-all-nemeses"; fi
    uv run scripts/fault_rig.py run --cycles {{cycles}} $require {{args}}

# The rig's own canary: damage planted in the replica (a dropped, swapped or flipped level-0 file)
# must be reported. A rig that reports 0 anomalies here is blind.
fault-rig-canary *args: minio-up toxiproxy-up
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --release --locked -p lsr-cli
    cargo build --release --locked -p lsr-history --features sqlite
    ./scripts/build-go-litestream.sh 041a8c3
    eval "$(./scripts/minio.sh env)"
    eval "$(./scripts/toxiproxy.sh env)"
    uv run scripts/fault_rig.py canary {{args}}

# The trial drill (scripts/trial_drill.py, docs/trial-drill.md): the pinned Go build replicates a
# synthetic database of the given GiB to local MinIO, lsr takes over under load (with kill -9 during a
# large upload, a compaction and a snapshot), both binaries restore and are compared with a ledger of
# commits, and Go takes it back. Defaults are Litestream's own config and take hours; arguments after the
# size (durations, intervals, budgets, --work DIR) go to the script. Exit 1 on a failed phase or budget.
trial-drill gib *args: minio-up
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --release --locked -p lsr-cli
    ./scripts/build-go-litestream.sh 041a8c3
    eval "$(./scripts/minio.sh env)"
    uv run scripts/trial_drill.py run --gib {{gib}} {{args}}

# The same drill against a real provider's bucket (tigris, aws or r2), under one run prefix that
# scripts/provider.sh deletes afterwards, multipart uploads included, failing if it lists non-empty.
trial-drill-provider provider gib *args:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --release --locked -p lsr-cli
    ./scripts/build-go-litestream.sh 041a8c3
    ./scripts/provider.sh {{provider}} uv run scripts/trial_drill.py run --store provider --gib {{gib}} {{args}}

# The drill's own canary: damage planted in a restored database (a flipped byte, a state that never
# existed, a state newer than the instant) must be reported, and the intact database must pass.
trial-drill-canary:
    uv run scripts/trial_drill.py canary

# The nightly's small run: 0.05 GiB on MinIO behind a 4 MiB/s upload limit, with every interval
# compressed to seconds so the snapshot, level 3 and the retention pass all happen in minutes.
trial-drill-small:
    just trial-drill 0.05 --work target/trial-drill/nightly --throttle-mib-s 4 --snapshot-interval 90s --snapshot-retention 300s --levels 10s,20s,40s --l0-check 10s --l0-retention 30s --sync-interval 1s --part-size 8MB --truncate-page-n 250000 --burst-mib 12 --go-seconds 20 --min-run 4 --max-run 20 --budget-catchup 180 --first-timeout 600

# The fault rig against a planted engine defect (docs/planted-bugs/<name>.patch), until its first
# anomaly: just fault-rig-plant restart-abandoned-at-synced-end 200
fault-rig-plant name cycles="200" *args: minio-up toxiproxy-up
    #!/usr/bin/env bash
    set -euo pipefail
    patch="docs/planted-bugs/{{name}}.patch"
    [ -f "$patch" ] || { echo "fault-rig-plant: no plant named {{name}}" >&2; exit 2; }
    tree=".worktrees/fault-rig-plant-{{name}}"
    git worktree add --detach "$tree" HEAD >/dev/null
    trap 'git worktree remove --force "$tree"' EXIT
    git -C "$tree" apply "$PWD/$patch"
    # The judge comes from this tree, never the planted one.
    cargo build --release --locked -p lsr-history --features sqlite
    # A plant that does not build is a broken plant (re-express it, docs/planted-bugs.md), never
    # "the rig missed it": exit 3, named, apart from the rig's own 0 (not found), 1 (found), 2.
    if ! CARGO_TARGET_DIR="$PWD/target/fault-rig-plants/{{name}}" cargo build --release --locked -p lsr-cli --manifest-path "$tree/Cargo.toml"; then
        echo "fault-rig-plant: PLANT {{name}} DID NOT BUILD with its patch applied; re-express docs/planted-bugs/{{name}}.patch" >&2
        exit 3
    fi
    ./scripts/build-go-litestream.sh 041a8c3
    eval "$(./scripts/minio.sh env)"
    eval "$(./scripts/toxiproxy.sh env)"
    # Linux: LazyFS is built when missing, and a menu nemesis that
    # cannot run fails the step: "not found" with power loss never run says nothing.
    require=""
    if [ "$(uname)" = Linux ]; then
        test -x "${LSR_LAZYFS_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/lsr-lazyfs}/lazyfs/lazyfs/build/lazyfs" || uv run scripts/lazyfs.py build
        require="--require-all-nemeses"
    fi
    uv run scripts/fault_rig.py run --cycles {{cycles}} --stop-on-anomaly $require --lsr "target/fault-rig-plants/{{name}}/release/lsr" --work "target/fault-rig-plant-{{name}}" {{args}}

# A plant is scripts/lazyfs-plants/<name>.patch or docs/planted-bugs/<name>.patch, applied to a
# scratch worktree of HEAD and built into target/lazyfs-plants/<name>.
# The LazyFS rig against a planted defect, until its first anomaly: just lazyfs-plant wal-barrier 200
lazyfs-plant name cycles="200" *args:
    #!/usr/bin/env bash
    set -euo pipefail
    patch="scripts/lazyfs-plants/{{name}}.patch"
    [ -f "$patch" ] || patch="docs/planted-bugs/{{name}}.patch"
    [ -f "$patch" ] || { echo "lazyfs-plant: no plant named {{name}}" >&2; exit 2; }
    tree=".worktrees/lazyfs-plant-{{name}}"
    git worktree add --detach "$tree" HEAD >/dev/null
    trap 'git worktree remove --force "$tree"' EXIT
    git -C "$tree" apply "$PWD/$patch"
    # The judge comes from this tree, never the planted one.
    cargo build --release --locked -p lsr-history --features sqlite
    CARGO_TARGET_DIR="$PWD/target/lazyfs-plants/{{name}}" cargo build --release --locked -p lsr-cli --manifest-path "$tree/Cargo.toml"
    test -x "${LSR_LAZYFS_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/lsr-lazyfs}/lazyfs/lazyfs/build/lazyfs" || uv run scripts/lazyfs.py build
    uv run scripts/lazyfs.py run --cycles {{cycles}} --stop-on-anomaly --lsr "target/lazyfs-plants/{{name}}/release/lsr" --work "target/lazyfs-plant-{{name}}" {{args}}