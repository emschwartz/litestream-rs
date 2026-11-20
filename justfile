# Every recipe is scoped so it can run inside a worktree.
default: check

# The land gate: format, lints, tests. A branch lands only when this is green.
check: fmt-check clippy test

fmt-check:
    cargo fmt --all -- --check

clippy:
    cargo clippy --workspace --all-targets --locked -- -D warnings

test:
    cargo nextest run --workspace --locked --no-tests=warn

# A branch in its own worktree under the main checkout's .worktrees/, off main's tip.
spawn name:
    git -C "{{justfile_directory()}}" worktree add ".worktrees/{{name}}" -b "{{name}}" main

# Build the pinned Go binaries, then run the live checks against them (not part of check).
compat:
    ./scripts/build-go-litestream.sh
    cargo nextest run -p lsr-compat --run-ignored all --locked

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

# Pinned upstream sources, read-only reference for the port and input to the compat harness.
upstream:
    ./scripts/fetch-upstream.sh
