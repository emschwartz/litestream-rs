# AGENTS.md

**litestream-rs** (working name): a Rust port of Litestream 0.5.x, as a library and as a sidecar. [README.md](README.md) says what it is and how it works; this file is the rules for changing it. Read `docs/PLAN.md` before design work; it fixes decisions that task plans may not reopen.

## The contract
- The replica is the contract: bucket layout plus LTX v3 files. Tolerant reader (everything Go v0.5.0 and later wrote), strict writer (Go v0.5.16 and later must read it).
- v1 adds no file, header, metadata or object Go does not speak; ideas that need one go in `docs/proposals/beyond-go.md`.
- Behaviour may differ from Go where Go is wrong. Never silently wrong: a request that cannot be met exactly is an error, never a shorter history.
- Equality with Go is defined on restored database pages, never on LTX bytes (the LZ4 compressors differ).

## Design rules
- Invariants live in types. A value that exists is valid: constructors return `Option` or `Result`, fields are private. Prefer a type that cannot hold the bad state over a runtime check, and a runtime check over an assert.
- Closed enums for states, faults and causes; every `match` on them is exhaustive (the wildcard-arm lints enforce it). A new variant must break the build everywhere it matters.
- Sans-IO core: decisions are pure functions over typed inputs; I/O sits behind traits (`Clock`, `Sleeper`, `Entropy`, `ObjectStore` for the replica and for local staging, `WalSource`, `PageSource`, `Checkpointer`, `DatabaseSink`) so the simulator can drive every path.
- Never use bytes that were not verified on the read that produced them.
- Bounded everything: channels, retries, fan-in, memory per file. No unbounded queue. Every retry wait is capped, and a failure that persists is a typed degraded state the health handle reports; it never retries silently and never spins.
- No panic in release builds. Assertions come in three tiers: `debug_assert!` for cheap pre and postconditions (positive and negative space); `invariant!(cond, BugSite::X)` for invariants whose violation release must report, a panic in debug and test builds and `Fault::Bug` in release, which rebuilds the engine instead of killing the host; the `verify` feature (`lsr_core::VERIFY`, on in the sim, fuzz and tests, off in release) for expensive checks.
- No caching and no parallelism unless a measurement in the PR justifies it.

## Rust style
- The lint set is in `Cargo.toml`; gates run with `-D warnings`. No `unwrap`, `expect`, `panic`, `as`, indexing or unchecked arithmetic in non-test code. An exception is `#[expect(lint, reason = "the invariant")]` on the smallest item, never `allow`.
- Functions are at most 70 lines.
- `unsafe` is denied workspace-wide; only the SQLite crate may opt in, one reasoned `#[expect(unsafe_code)]` per site. The one other exception is `lsr-heap`, the counting allocator the memory gates install (`GlobalAlloc` is an unsafe trait); it is a dev-dependency and never linked into a shipped binary.
- Errors are typed (`thiserror`), one enum per crate boundary, each variant carrying what a caller needs to act. No stringly errors, no `anyhow` in library crates.
- Every public item has a doc comment. Comments say why, never what. Mechanism goes in the module's `//!` doc.
- No mid-sentence em dashes in docs or comments.

## Layout
- Crates are `crates/<name>`, package `lsr-<name>`; `docs/PLAN.md` section 3 says what each owns. `replica` is the library API and `cli` the `lsr` binary. `sim`, `spec`, `compat`, `fuzzing` and `heap` are test-only. `fuzz/` (cargo-fuzz) is outside the workspace.
- Crate rules beyond the workspace's:
  - `sqlite`, `sidecar`: no descriptor on a database file or its `-shm` is ever opened and closed (closing one drops every POSIX lock the process holds on the file). Each has its own `clippy.toml`, which repeats the root file (clippy reads only the nearest).
  - `spec`: no dependencies, never `lsr-core`. It is the oracle the restore planner is judged by; change it only when the contract in the `crates/core/src/plan.rs` module doc changes.
- `just upstream` clones the pinned Go sources into `.upstream/` (read-only: `litestream` at 041a8c3, `ltx` at v0.5.3). Cite Go by file and line there, and check a claim about Go against it. [docs/go-terms.md](docs/go-terms.md) maps our terms to Go's.

## Commands and tests
- `just check` is the land gate: format, clippy, every test that is not ignored (nextest), doctests. While iterating: `cargo clippy -p <crate> --all-targets -- -D warnings`, `cargo nextest run -p <crate> <test name>`, `cargo test --doc -p <crate>` (nextest runs no doctests).
- [docs/TESTING.md](docs/TESTING.md) has every other recipe by what it needs (Docker, Go, Tigris, nightly Rust), the test rules, and the planted-bug and mutants workflows.
- The rules that bind every change: a bug fix starts with a test that fails on the old code and adds a planted-bug entry in the same commit; tests wait on conditions, never on the clock; a test that needs something outside the process fails without it, never skips; a crate's integration tests are one binary (`tests/<name>/main.rs`, a module per file).

## Dependencies
- New runtime dependencies: prefer none. Dev- and test-only dependencies that strengthen correctness checking (fuzzing, schedule exploration, sanitizer support) are welcome. Either way, each one is named in the report with why and its license (`deny.toml` holds the allowlist).
- Every dependency, external or `lsr-*`, is declared once in the root `[workspace.dependencies]` and referenced as `name.workspace = true`; a crate may add features, never a version or path. (The `fuzz/` crate keeps its own.)
- The resolver refuses crates.io versions published less than 3 days ago (`.cargo/config.toml`).

## Branches and landing
- Work lands on `main` of `origin` (github.com/emschwartz/litestream-rs), which stays green.
- Work happens on a branch in a worktree under `.worktrees/<name>` (`just spawn <name>`, from the main checkout), with `CARGO_TARGET_DIR` left at the default. A worktree shares the main checkout's `.upstream/`.
- A branch is squashed to a few logical commits and handed over with `ready to land: <branch> @<sha>`. The parent session runs `just check`, fast-forwards `main` and pushes it to `origin`. Never merge commits.
- A session working on a branch commits there and does not push, open pull requests or comment on GitHub unless asked; landing and pushing are the parent session's.
- Commit messages follow Conventional Commits: a subject `type(scope): summary`, imperative, lower-case after the colon, at most 72 characters, no trailing period; a blank line; a body that says why; optional footers.
  - Types: `feat` (new behaviour), `fix` (a bug), `perf`, `refactor` (no behaviour change), `test`, `docs`, `build` (dependencies, Cargo, the justfile), `ci`, `chore`, `revert`.
  - Scope: the crate without its `lsr-` prefix (`ltx`, `wal`, `core`, `store-s3`, `store-fs`, `local`, `sqlite`, `replica`, `config`, `sidecar`, `sim`, `spec`, `compat`, `cli`, `fuzzing`, `heap`), or `plan` for `docs/PLAN.md` and `docs/plans/`, or `deps`, `ci`, `lint`. Omit it only when a change spans the whole workspace.
  - A change that breaks a public API or the replica format takes `!` after the scope and a `BREAKING CHANGE:` footer saying what breaks.
  - Decisions made mid-task are `Call: <what> (why; cost if wrong)` footers.
  - `just commit-check [base]` checks the branch's subjects since `main`.
- Write every commit message for an outsider who has not read the plans: say what the change does to the code or the behaviour, in plain words. No plan shorthand in the subject or body (task numbers like "task 8t" or "item 10", ruling and decision codes like P5, D7 or F1, spike or gap ids like S1 or G4, "phase 2 exit"). When the plan's reasoning matters, explain it in a sentence and point at the document by path, for example "see docs/plans/phase-3-syncing.md". Example: `fix(store-s3): send the signed content length on empty uploads`.

## Done
A task ends at a verified commit: `just check` green on the branch, tests named in the task present and passing, docs updated where behaviour is described.
