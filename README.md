# litestream-rs (working name)

An in-process Rust library that replicates a WAL-mode SQLite database to S3-compatible storage and restores from it, interchangeable with [Litestream](https://github.com/benbjohnson/litestream) 0.5.x on the same bucket. Not affiliated with or endorsed by the Litestream project.

Status: early. Nothing here is ready to protect data.

The contract is the replica, not Litestream's behaviour: this library reads whatever Go Litestream wrote, and Go `litestream restore` and the Go VFS read whatever this library writes.

- Plan, design goals and phases: `docs/PLAN.md`
- Rules for working in this repository: `AGENTS.md`

## Crates

Each crate is `crates/<name>`, package `lsr-<name>`. The crate map, with what each owns and the rules it follows, is `docs/PLAN.md` section 3. Entry points: `lsr` (the `cli` crate) is the binary, `replica` is the library API.

## SQLite builds

The library and the application share one SQLite: `libsqlite3-sys` declares `links = "sqlite3"`, so a build holds one version of it. `lsr-replica` accepts `libsqlite3-sys` 0.37 to 0.40 (SQLite 3.51.3 to 3.54.1, never 3.51.2, which shipped a WAL-reset bug), so an application on sqlx 0.9 (`just sqlx-check` builds one) or on rusqlite 0.39 to 0.43 builds beside it.

- **Bundled (the default):** the `bundled` feature compiles the SQLite that `libsqlite3-sys` bundles. Every such build in the range is verified.
- **Your own SQLite:** `lsr-replica = { default-features = false }` links the system's or the application's SQLite instead. It must be one of the verified builds, or `Replicator::start` refuses it with `StartError::SqliteBuild`, naming the version found and the versions verified.
- **Verified builds:** 3.51.3, 3.53.1, 3.53.2, 3.54.0 and 3.54.1, each by version and source id (`crates/sqlite/src/version.rs`). The engine reads SQLite's private file and wal-index layouts, so each build is added only after those reads are checked against its source and the whole `lsr-sqlite` suite has passed on it in CI (`just sqlite-build`, `docs/decisions/D200.md`).
- **Connections:** `app_connection_raw` sets up a connection from any binding's raw handle; `Replicator::app_connection` takes a rusqlite connection (feature `rusqlite`).

## Tests by what they need

| Needs | Command | What runs |
|---|---|---|
| Nothing | `just check` | Format, clippy and every test that is not ignored over all features, doctests, the S3 client builds with each HTTP client alone, and the test map's counterparts against a fresh test list (Linux). The land gate. |
| Nothing | `just shuttle` | Schedule exploration of the replica thread and the S3 batch under a `--cfg shuttle` build of its own. Not in `just check`; CI runs it on pull requests that touch `crates/store-s3` or `crates/replica`, and nightly runs far more schedules. |
| Nothing, minutes | `just sqlite-big`, `just mem-big`, `just sim-soak <secs>`, `just sim-nightly`, `just fuzz-seed`, `just mutants <crate>` | The large real-SQLite cases (several GB of disk), the memory gates at 1 to 4 GiB and a million files, rotating simulator seeds, the fuzz seed corpus, mutation testing. |
| Docker | `just test-s3`, `just compat-s3` | The S3 store, `lsr` and the history judge's real-engine run (and Go and Rust together) against a throwaway MinIO. Fails when Docker is down; `LSR_SKIP=minio just test-s3` opts out and runs nothing. `just test-s3` also needs Go, to build the pinned Toxiproxy (MIT licence) that the history judge's network-fault tests put between the engine and MinIO; `LSR_SKIP=toxiproxy` opts out of those. |
| Docker, Go | `just fault-rig <cycles>`, `just fault-rig-canary`, `just fault-rig-plant <name>` | The real `lsr replicate` beside a SQLite writer with its replica on MinIO behind Toxiproxy, under a seeded menu (kill -9, partitions, slow and cut transfers, the pinned Go Litestream on the same prefix; LazyFS power loss and a libfaketime clock step on Linux), judged by point-in-time restores (`scripts/fault_rig.py`, `docs/decisions/D195.md`). Nightly runs 1,000 cycles. Exit 1 on an anomaly, whose evidence is kept under `target/fault-rig/evidence`. |
| Go | `just upstream`, then `just compat`, `just compat-live <names>`, `just compat-releases`, `just compat-tags`, `just compat-regen` | Pinned Go builds from `.upstream/` (a worktree shares the main checkout's): Go writes and Rust restores, the reverse, handovers and alternation, the Go VFS (which also needs a C compiler and SQLite headers), and the effective-config differentials against a dumper built from the pinned Go source (`crates/compat/src/dump.rs`). `compat-tags` needs only the clone. |
| A real provider (Tigris, AWS S3, Cloudflare R2) | `just test-provider <tigris\|aws\|r2>`, `just test-tigris`, `just tigris-restore`, `just soak-tigris <hours>` | The real bucket, with credentials from `~/.config/litestream-rs/<provider>.env` (`scripts/provider.sh`; Tigris also takes `AWS_*`). `test-provider` runs the conformance suite, the create-only write and race checks, consistency, error classification, abandoned uploads and one `lsr replicate` and restore, under a run prefix it empties afterwards; `test-tigris` is its Tigris case. `tigris-restore` also builds Go v0.5.18. Never in CI. |
| Nightly Rust | `just fuzz <target> <secs>`, `just fuzz-cmin <target>` | cargo-fuzz over `fuzz/`. |

The gate is dominated by compiling, so a compiler cache helps on a machine that builds many worktrees: `cargo install sccache --locked`, then `export RUSTC_WRAPPER=sccache SCCACHE_DIR=~/.cache/sccache` (one directory shared by every checkout) before `just check`. It is optional; nothing in the repo requires it.

A test that needs something outside the process (MinIO, the Go builds, Tigris credentials, a C compiler, free disk) fails when it is missing; it never passes by skipping. The one opt-out is the runner naming it: `LSR_SKIP=minio` (comma separated: `minio`, `toxiproxy`, `go`, `tigris`, `aws`, `r2`, `cc`, `disk`). CI never sets it, and under CI (the `CI` variable) setting it is itself a failure. The rule lives in `lsr_core::needs`. A test that hangs is killed by nextest (`.config/nextest.toml`) and CI jobs carry `timeout-minutes`, so a hang is a failure, not a six-hour job.
