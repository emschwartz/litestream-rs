# litestream-rs (working name)

An in-process Rust library that replicates a WAL-mode SQLite database to S3-compatible storage and restores from it, interchangeable with [Litestream](https://github.com/benbjohnson/litestream) 0.5.x on the same bucket. Not affiliated with or endorsed by the Litestream project.

Status: early. Nothing here is ready to protect data.

The contract is the replica, not Litestream's behaviour: this library reads whatever Go Litestream wrote, and Go `litestream restore` and the Go VFS read whatever this library writes.

- Plan, design goals and phases: `docs/PLAN.md`
- Rules for working in this repository: `AGENTS.md`

## Crates

Each crate is `crates/<name>`, package `lsr-<name>`. Listed from the bottom of the dependency graph up; the rules each crate follows are in `docs/PLAN.md` section 3.

| Crate | What it is |
|---|---|
| `ltx` | The LTX v3 file format: identifiers, checksums, the decoder and the strict encoder. No I/O beyond `Read` and `Write`. |
| `wal` | The SQLite WAL reader: header and frame verification, committed transactions, page maps. |
| `core` | The sans-IO engine: position recovery, sync planning, checkpoint decisions, uploads, compaction, retention, restore planning and verify, behind the `Clock`, `ObjectStore`, `WalSource` and other traits. |
| `store-fs` | `ObjectStore` on a directory laid out as Go's file replica. |
| `store-s3` | `ObjectStore` on an S3-compatible bucket: sans-IO requests and responses, a blocking `ureq` transport by default, `reqwest` as a feature, its own retry classifier. |
| `local` | The local file system side: the sink a restore builds its database in, published by hard link. |
| `sqlite` | The engine's doors on a live SQLite database: the read transaction that pins the WAL, the wal-index, pages and checkpoints. The only crate with `unsafe`. |
| `replica` | The public API: `Replicator` (one thread per database), its health and events, `restore` and `verify`. |
| `config` | Go's `litestream.yml` read as Go reads it, mapped onto replicators, and the one `s3://` URL parser. |
| `sidecar` | The `replicate` daemon: Go's command line and config, many databases in one process, signals, `-exec`, `/metrics` and `/healthz`. |
| `cli` | The `lsr` binary: `restore`, `verify`, `files`, `config check`, and `replicate`, which is the sidecar. |
| `sim` | Dev only: the deterministic simulator, a model of SQLite's WAL checked against SQLite, the oracle. |
| `compat` | Dev only: pinned Go binaries, seeded workloads, golden fixtures, the cross-implementation tests and the upstream-test ledger. |
| `fuzzing` | Dev only: the fuzz targets as plain functions, replayed over the committed corpus in the test suite; `fuzz/` drives them with cargo-fuzz. |

## Tests by what they need

| Needs | Command | What runs |
|---|---|---|
| Nothing | `just check` | Format, clippy, every test that is not ignored, doctests, the S3 client feature builds, and the ledger's counterparts against a fresh test list (Linux). The land gate. |
| Nothing, minutes | `just sqlite-big`, `just sim-soak <secs>`, `just sim-nightly`, `just fuzz-seed`, `just mutants <crate>` | The large real-SQLite cases (several GB of disk), rotating simulator seeds, the fuzz seed corpus, mutation testing. |
| Docker | `just test-s3` | The S3 store and `lsr` against a throwaway MinIO; skipped when Docker is down. |
| Go | `just upstream`, then `just compat`, `just compat-live <names>`, `just compat-releases`, `just compat-tags`, `just compat-regen` | Pinned Go builds from `.upstream/` (a worktree shares the main checkout's): Go writes and Rust restores, the reverse, handovers and alternation, the Go VFS (which also needs a C compiler and SQLite headers). `compat-tags` needs only the clone. |
| Tigris | `just test-tigris`, `just tigris-restore`, `just soak-tigris <hours>` | The real bucket, with credentials from a file outside the repo or `AWS_*`; `tigris-restore` also builds Go v0.5.18. Never in CI. |
| Nightly Rust | `just fuzz <target> <secs>`, `just fuzz-cmin <target>` | cargo-fuzz over `fuzz/`. |
