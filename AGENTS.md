# AGENTS.md

**litestream-rs** (working name): a Rust port of Litestream 0.5.x, as a library and as a sidecar. [README.md](README.md) says what it is, how it works and how it differs from Go; this file is the rules for changing it. Read `docs/PLAN.md` before design work; it fixes decisions that task plans may not reopen.

## The contract
- The replica is the contract: bucket layout plus LTX v3 files. Tolerant reader (everything Go v0.5.0 and later wrote), strict writer (Go v0.5.16 and later must read it).
- v1 adds no file, header, metadata or object Go does not speak; ideas that need one go in `docs/proposals/beyond-go.md`.
- Behaviour may differ from Go where Go is wrong. Never silently wrong: a request that cannot be met exactly is an error, never a shorter history.
- Equality with Go is defined on restored database pages, never on LTX bytes (the LZ4 compressors differ).

## Design rules
- Invariants live in types. A value that exists is valid: constructors return `Option` or `Result`, fields are private. Prefer a type that cannot hold the bad state over a runtime check, and a runtime check over an assert.
- Closed enums for states, faults and causes; every `match` on them is exhaustive (the wildcard-arm lints enforce it). A new variant must break the build everywhere it matters.
- Sans-IO core: decisions are pure functions over typed inputs; I/O sits behind traits (`Clock`, `Sleeper`, `Entropy`, `ObjectStore` for the replica and for local staging, `WalSource`, `PageSource`, `Checkpointer`, `DatabaseSink`) so the simulator can drive every path. The root `clippy.toml` disallows reading a clock or sleeping directly.
- Never use bytes that were not verified on the read that produced them.
- Bounded everything: channels, retries, fan-in, memory per file. No unbounded queue. Every retry wait is capped, and a failure that persists is a typed degraded state the health handle reports; it never retries silently and never spins.
- No panic in release builds. Assertions come in three tiers: `debug_assert!` for cheap pre and postconditions (positive and negative space); `invariant!(cond, BugSite::X)` for invariants whose violation release must report, a panic in debug and test builds and `Fault::Bug` in release, which rebuilds the engine instead of killing the host; the `verify` feature (`lsr_core::VERIFY`, on in the sim, fuzz and tests, off in release) for expensive checks.
- No caching and no parallelism unless a measurement in the PR justifies it.

## Rust style
- The lint set is in `Cargo.toml`; gates run with `-D warnings`. No `unwrap`, `expect`, `panic`, `as`, indexing or unchecked arithmetic in non-test code. An exception is `#[expect(lint, reason = "the invariant")]` on the smallest item, never `allow`.
- Functions are at most 70 lines.
- `unsafe` is denied workspace-wide; only the SQLite crate may opt in, one reasoned `#[expect(unsafe_code)]` per site. The one other exception is `lsr-heap`, the counting allocator the memory gates install (`GlobalAlloc` is an unsafe trait); it is a dev-dependency and never linked into a shipped binary.
- Errors are typed (`thiserror`), one enum per crate boundary, each variant carrying what a caller needs to act. No stringly errors, no `anyhow` in library crates.
- Every public item has a doc comment. Comments say why, never what. Mechanism goes in the module's `//!` doc, which is the reference for that module: update it with the behaviour.
- No mid-sentence em dashes in docs or comments.

## Layout
- Crates are `crates/<name>`, package `lsr-<name>` (working prefix); the README's "Repository" table says what each is, `docs/PLAN.md` section 3 what each owns. `replica` is the library API and `cli` the `lsr` binary. `sim`, `spec`, `compat`, `fuzzing` and `heap` are test-only. `fuzz/` (cargo-fuzz, nightly) is outside the workspace with its own lockfile.
- Crate rules beyond the workspace's:
  - `sqlite`, `sidecar`: no descriptor on a database file or its `-shm` is ever opened and closed (closing one drops every POSIX lock the process holds on the file). Each has its own `clippy.toml` that disallows the `std::fs` opens; clippy reads only the nearest one, so it repeats the root file in full.
  - `spec`: no dependencies, never `lsr-core`. It is the independent oracle the restore planner is judged by; change it only when the documented contract (the `crates/core/src/plan.rs` module doc) changes.
- `just upstream` clones the pinned Go sources into `.upstream/` (read-only: `litestream` at 041a8c3, `ltx` at v0.5.3). Cite Go by file and line there. [docs/go-terms.md](docs/go-terms.md) maps our terms to Go's, LTX's and SQLite's; check it before coining a term.

## Commands
`just check` is the land gate: format, clippy over all features, every test that is not ignored (nextest), doctests, the S3 store's feature builds, and the upstream test map. It needs nothing outside the process and passes on Linux and macOS (on macOS it skips `upstream-test-map-fresh`). While iterating: `cargo clippy -p <crate> --all-targets -- -D warnings`, `cargo nextest run -p <crate> <test name>`, `cargo test --doc -p <crate>` (nextest runs no doctests).

| Needs | Recipes | What runs |
|---|---|---|
| Nothing | `just check`, `just deny`, `just commit-check` | The gate; advisories, licenses and sources (`cargo-deny`); commit subjects |
| Nothing, its own build | `just shuttle` | Thread-schedule exploration of the replica thread and the S3 batch. CI runs it on pull requests that touch `store-s3` or `replica` |
| Nothing, minutes | `just sim-soak <secs>`, `just sim-nightly`, `just plan-exhaustive`, `just sqlite-big`, `just mem-big`, `just fuzz-seed` | Rotating simulator seeds, planner sweeps, multi-GB SQLite cases, memory gates at scale, the fuzz seed corpus |
| Nothing, long | `just mutants <crate>`, `just mutants-diff`, `just planted-bugs [--only NAME]` | Mutation testing; the planted-bug ledger (about 20 minutes cold, needs `uv`) |
| Nightly Rust | `just fuzz <target> <secs>`, `just fuzz-cmin <target>`, `just asan`, `just tsan`, `just miri` | cargo-fuzz, sanitizers, Miri |
| Docker | `just test-s3`, `just compat-s3` | The S3 store and `lsr`, then Go and Rust together, against a throwaway MinIO |
| Go | `just upstream`, then `just compat`, `just compat-live <names>`, `just compat-releases`, `just compat-alternation <seed>`, `just compat-regen` | Pinned Go builds from `.upstream/`: each restores the other's files, handovers, alternation, the Go VFS (also needs a C compiler and SQLite headers) |
| Tigris credentials | `just test-tigris`, `just tigris-restore`, `just soak-tigris <hours>` | The real bucket. Never in CI |

`just sim-replay <seed> [steps]` replays one simulator run, printing every operation; a failing run prints its replay line. sccache is optional and helps across worktrees (`RUSTC_WRAPPER=sccache`, one shared `SCCACHE_DIR`).

## Tests
- Test the contract, not the implementation. Every decoder error variant has a test that produces it; every `Fault` and `SnapshotCause` variant has a simulator scenario (tests in `crates/sim` fail on a variant without one).
- A bug fix starts with a test that fails on the old code, and adds an entry to `docs/planted-bugs.toml` with a patch in `docs/planted-bugs/` that re-introduces the defect, in the same commit ([docs/planted-bugs.md](docs/planted-bugs.md)). Nightly CI re-plants each one and fails if its tests pass. A `STALE` patch is re-expressed against the current tree.
- A mutant missed in a changed line of `core` or `ltx` fails the pull request (`just mutants-diff`). Kill it with a test; only a true equivalent goes in `docs/mutants-escapes.txt`, with its reason in [docs/mutants.md](docs/mutants.md), in one commit.
- Property tests (`proptest`) for every codec and every pure planner. Fuzz targets are plain functions in `crates/fuzzing`; the committed corpus replays in `just check` ([docs/fuzzing.md](docs/fuzzing.md)).
- Go compatibility is differential: generate with Go, read with Rust, and the reverse, comparing restored pages. Every upstream Go test is mapped to a counterpart or a reason in `docs/upstream-test-map.toml`; renaming or deleting a counterpart fails the gate.
- Fixtures are synthetic databases or files generated by pinned Go tools. Never a capture of anyone's real data.
- Tests wait on conditions, never on the clock. No sleeps. Keep each test under a second where possible.
- A test that needs something outside the process (MinIO, Go builds, Tigris, a C compiler, disk) gets it through `lsr_core::needs` and fails without it; it never passes by skipping. The only skip is the runner's `LSR_SKIP=<name>` (`minio`, `go`, `tigris`, `cc`, `disk`), and under CI setting it is itself a failure. A walk over files or fixtures asserts it found some.
- A hang is a failure: nextest kills a test after its limit (`.config/nextest.toml`, 180 s by default, longer for the recipe-only suites) and CI jobs carry `timeout-minutes`.
- A crate's integration tests are one binary, `crates/<name>/tests/<name>/main.rs`, with a module per file: add a module there, not a new `tests/*.rs` file, which would link a binary of its own (lsr-compat's `layout` test refuses a second one). A test that starts the binary again as a child names its test by full path (`sidecar::exec_child`), or uses the crate root's `child_process_entry` (lsr-sqlite).

## Dependencies
- New runtime dependencies: prefer none. Dev- and test-only dependencies that strengthen correctness checking (fuzzing, schedule exploration, sanitizer support) are welcome. Either way, each one is named in the report with why and its license; `deny.toml` holds the license allowlist.
- Every dependency, external or `lsr-*`, is declared once in the root `[workspace.dependencies]` and referenced as `name.workspace = true`; a crate may add features, never a version or path. (The `fuzz/` crate keeps its own.)
- The resolver refuses crates.io versions published less than 3 days ago (`.cargo/config.toml`). The toolchain is pinned to a full version in `rust-toolchain.toml`.

## Branches and landing
- The repository lives on GitHub (`origin`, github.com/emschwartz/litestream-rs); work lands on its `main`, which stays green.
- Work happens on a branch in a worktree under `.worktrees/<name>` (`just spawn <name>`, from the main checkout), with its own `CARGO_TARGET_DIR` left at the default inside the worktree. A worktree shares the main checkout's `.upstream/` (the scripts and the compat harness find it there).
- A branch is squashed to a few logical commits and handed over with `ready to land: <branch> @<sha>`. The parent session runs `just check`, fast-forwards `main` and pushes it to `origin`. Never merge commits.
- A session working on a branch commits there and does not push, open pull requests or comment on GitHub unless asked; landing and pushing are the parent session's.
- Commit messages follow Conventional Commits (commitizen's default format): a subject `type(scope): summary`, imperative, lower-case after the colon, at most 72 characters, no trailing period; a blank line; a body that says why; optional footers.
  - Types: `feat` (new behaviour), `fix` (a bug), `perf`, `refactor` (no behaviour change), `test`, `docs`, `build` (dependencies, Cargo, the justfile), `ci`, `chore`, `revert`.
  - Scope: the crate without its `lsr-` prefix (`ltx`, `wal`, `core`, `store-s3`, `store-fs`, `local`, `sqlite`, `replica`, `config`, `sidecar`, `sim`, `spec`, `compat`, `cli`, `fuzzing`, `heap`), or `plan` for `docs/PLAN.md` and `docs/plans/`, or `deps`, `ci`, `lint`. Omit it only when a change spans the whole workspace.
  - A change that breaks a public API or the replica format takes `!` after the scope and a `BREAKING CHANGE:` footer saying what breaks.
  - Decisions made mid-task are `Call: <what> (why; cost if wrong)` footers.
  - `just commit-check` checks the subjects of the commits on the branch since local `main` (or `origin/main` when there is none; `just commit-check <base>` names another).
- Write every commit message for an outsider who has not read the plans: say what the change does to the code or the behaviour, in plain words. No plan shorthand in the subject or body (task numbers like "task 8t" or "item 10", ruling and decision codes like P5, D7 or F1, spike or gap ids like S1 or G4, "phase 2 exit"). When the plan's reasoning matters, explain it in a sentence and point at the document by path, for example "see docs/plans/phase-3-syncing.md". Example: `fix(store-s3): send the signed content length on empty uploads`.

## Done
A task ends at a verified commit: `just check` green on the branch, tests named in the task present and passing, docs updated where behaviour is described (the module doc, and the README where it states the behaviour).
