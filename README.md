# litestream-rs (working name)

An in-process Rust library that replicates a WAL-mode SQLite database to S3-compatible storage and restores from it, interchangeable with [Litestream](https://github.com/benbjohnson/litestream) 0.5.x on the same bucket. Not affiliated with or endorsed by the Litestream project.

Status: early. Nothing here is ready to protect data.

The contract is the replica, not Litestream's behaviour: this library reads whatever Go Litestream wrote, and Go `litestream restore` and the Go VFS read whatever this library writes.

- Plan, design goals and phases: `docs/PLAN.md`
- Rules for working in this repository: `AGENTS.md`

## Crates

Each crate is `crates/<name>`, package `lsr-<name>`. The crate map, with what each owns and the rules it follows, is `docs/PLAN.md` section 3. Entry points: `lsr` (the `cli` crate) is the binary, `replica` is the library API.

## Tests by what they need

| Needs | Command | What runs |
|---|---|---|
| Nothing | `just check` | Format, clippy, every test that is not ignored, doctests, the S3 client feature builds, and the test map's counterparts against a fresh test list (Linux). The land gate. |
| Nothing, minutes | `just sqlite-big`, `just mem-big`, `just sim-soak <secs>`, `just sim-nightly`, `just fuzz-seed`, `just mutants <crate>` | The large real-SQLite cases (several GB of disk), the memory gates at 1 to 4 GiB and a million files, rotating simulator seeds, the fuzz seed corpus, mutation testing. |
| Docker | `just test-s3`, `just compat-s3` | The S3 store and `lsr` (and Go and Rust together) against a throwaway MinIO. Fails when Docker is down; `LSR_SKIP=minio just test-s3` opts out and runs nothing. |
| Go | `just upstream`, then `just compat`, `just compat-live <names>`, `just compat-releases`, `just compat-tags`, `just compat-regen` | Pinned Go builds from `.upstream/` (a worktree shares the main checkout's): Go writes and Rust restores, the reverse, handovers and alternation, the Go VFS (which also needs a C compiler and SQLite headers). `compat-tags` needs only the clone. |
| Tigris | `just test-tigris`, `just tigris-restore`, `just soak-tigris <hours>` | The real bucket, with credentials from a file outside the repo or `AWS_*`; `tigris-restore` also builds Go v0.5.18. Never in CI. |
| Nightly Rust | `just fuzz <target> <secs>`, `just fuzz-cmin <target>` | cargo-fuzz over `fuzz/`. |

A test that needs something outside the process (MinIO, the Go builds, Tigris credentials, a C compiler, free disk) fails when it is missing; it never passes by skipping. The one opt-out is the runner naming it: `LSR_SKIP=minio` (comma separated: `minio`, `go`, `tigris`, `cc`, `disk`). CI never sets it, and under CI (the `CI` variable) setting it is itself a failure. The rule lives in `lsr_core::needs`. A test that hangs is killed by nextest (`.config/nextest.toml`) and CI jobs carry `timeout-minutes`, so a hang is a failure, not a six-hour job.
