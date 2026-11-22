# litestream-rs (working name)

A Rust port of [Litestream](https://github.com/benbjohnson/litestream) 0.5.x: it continuously copies a live SQLite database to S3-compatible storage (or a directory) and restores it to the latest state, a transaction, or a point in time. It is for Rust applications that want replication inside their own process, with no second program to deploy, and for anyone running Go Litestream's sidecar who wants a drop-in binary. Both modes run the same engine and write the same files as Go Litestream, so either implementation can restore, or take over, a replica the other wrote.

Not affiliated with or endorsed by the Litestream project.

**Status: early, not yet proven in production.** See [Status and limits](#status-and-limits).

| Mode | What you run | Use it when |
|---|---|---|
| Library | `lsr-replica`: one `Replicator` per database, on a thread of its own, no async runtime needed | The application is Rust and can call it at startup and shutdown |
| Sidecar | `lsr replicate`: a separate process that reads Go's `litestream.yml` and takes Go's `replicate` flags | The application is not Rust, or you run Go's sidecar today |
| Tools | `lsr restore`, `lsr verify`, `lsr files`, `lsr config check` | Restoring, or checking a replica either implementation wrote |

## Quick start

Nothing is published to crates.io yet, so both paths start from a checkout. You need the Rust toolchain that `rust-toolchain.toml` pins; SQLite is compiled in.

### The command line

```bash
cargo build --release -p lsr-cli        # the binary is target/release/lsr

# Replicate one database until a signal (file:// or s3://, as Go's replicate takes them).
lsr replicate /srv/app.db s3://my-bucket/app

# Or every database in a Go config file. Check the file first: it prints what a start would refuse.
lsr config check /etc/litestream.yml
lsr replicate -config /etc/litestream.yml

# Check a replica without restoring: reads every file, reports damage and gaps.
lsr verify --replica s3://my-bucket/app

# Restore. The output must not exist; a restore never overwrites a file.
lsr restore --replica s3://my-bucket/app --output /srv/restored.db
lsr restore --replica s3://my-bucket/app --output /srv/restored.db --timestamp 2026-11-20T12:00:00Z
```

S3 credentials come from the usual `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` variables (or `LITESTREAM_*`), or the AWS shared files. For a store that is not AWS, add `--endpoint`, or put `endpoint` in the config as Go's does. `lsr --help` and `lsr replicate -h` list every flag and exit code.

### The library

Depend on `lsr-replica` (with the `rusqlite` feature) and a store crate, `lsr-store-s3` or `lsr-store-fs`, by git or path. This is the example in [`crates/replica/src/lib.rs`](crates/replica/src/lib.rs), which the test suite compiles and runs:

```rust
use std::sync::Arc;
use std::time::Duration;

use lsr_replica::{Config, DynStore, Replicator, RestoreTarget, Stopped, restore};
use lsr_store_fs::FsStore;

// The database file exists before the replicator starts.
let setup = rusqlite::Connection::open(&db)?;
setup.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))?;
drop(setup);

// One store, one replicator per database, started before the application's connections open.
let store: Arc<dyn DynStore> = Arc::new(FsStore::new(&replica));
let replicator = Replicator::start(&db, Arc::clone(&store), Config::default())?;

// Every application connection: keep the WAL file, and leave checkpoints to the replicator.
let app = rusqlite::Connection::open(&db)?;
replicator.app_connection(&app)?;
app.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES (1);")?;

// At shutdown, after the writers finish: upload everything committed, or say what is left.
let stopped = replicator.stop(Duration::from_secs(30)).wait();
assert!(matches!(stopped, Stopped::Drained { txid: Some(_) }));

// A restore is a free function. It builds a new file and never touches an existing one.
restore(&*store, RestoreTarget::Latest, &restored)?;
```

For S3 the only change is the store: `Arc::new(lsr_store_s3::S3Store::new(config, credentials)?)`. `Config::default()` is Go Litestream's defaults. `replicator.health()` is a cheap snapshot to alert on: what is replicated, how far behind, and any failure it keeps retrying.

An embedding application owes the library four things:

- Start the replicator before opening any other connection, and pass every connection through `app_connection` (or `app_connection_raw` if it is not a `rusqlite` connection).
- Run no checkpoints of its own, and never open and close the database file with plain file I/O. Closing any file handle on a database drops every lock SQLite holds on it in that process.
- Call `stop` at shutdown, after its writers close.
- Build against the same bundled SQLite. `lsr-sqlite` uses `rusqlite` 0.43 with SQLite compiled in, and Cargo allows one SQLite per build.

## How it works

SQLite in WAL mode appends every change to a write-ahead log (the `-wal` file) and later copies it into the database file, a step called a checkpoint. Replication reads the log before that happens.

```
application ──writes──▶ app.db-wal ──poll 1 s, verify──▶ .app.db-litestream/ ──verify, upload──▶ level 0
                                                         (staging directory)                       │ merged every
                                                                                                   ▼ 30 s, 5 min, 1 h
                        app.db ─────────────── every 24 h, all pages ──────────────────▶ level 9   levels 1, 2, 3
```

**Capture.** The replicator holds a read transaction for its whole life, which stops SQLite from recycling log entries it has not copied yet. Every second it reads the new committed transactions, checking each log frame's checksum, and writes them as one file. It runs the checkpoints itself, and only after it has copied everything up to that point.

**LTX files and levels.** Each file is in LTX, Litestream's format: a set of changed pages covering a range of transaction ids, compressed and checksummed. Level 0 holds one small file per sync, and levels 1 to 3 hold merges of the level below over longer windows, so a restore reads a few large files instead of thousands of small ones. Level 9 holds snapshots: every page of the database.

**Staging directory.** New files land first in a directory beside the database (`.<name>-litestream`, Go's "meta directory"), are synced to disk, and are then uploaded in transaction order. After a restart the replicator resumes from the newest staged file. With no staging directory it resumes from the replica alone, when the log still matches.

**Snapshots.** A full copy is taken on the first sync, every 24 hours by default, and whenever the replicator cannot prove the log continues what it already copied. Each such case has a named cause in health and events, for example the log was deleted, another process checkpointed past the replicator, or a power loss rewound the log.

**Compaction and retention.** A merge takes at most 32 contiguous files and never deletes its inputs, so a crash at any point leaves a replica that restores. Retention deletes separately: snapshots older than the retention period (24 hours by default), the merged files only those snapshots needed, and level 0 files older than 5 minutes that a merged file covers. It always keeps the newest file of each level, and when a rule is ambiguous it keeps the file.

**Restore planning.** A restore lists the replica once and plans a chain: the file that starts at transaction 1 and reaches furthest, then each file that continues it. The target is the latest state, a transaction id (`--txid`), or an instant (`--timestamp`). If the replica cannot produce exactly that state the restore fails and says why: a gap in the history, a target past the newest file, or no starting file.

**Point in time.** A merged file carries only its newest input's time. When an instant falls inside a merged file whose inputs retention already removed, the exact state is not recoverable. The restore refuses, unless `--allow-earlier` accepts the newest state provably before the instant.

**Verification.** `lsr verify` decodes every file, checks its checksums and that its contents match its name, reports every gap, and then runs a real restore of the latest state into a sink that writes nothing. The library can do the same on a schedule (`validation.interval`, off by default as in Go) and compares the result with the live database.

### What is checked before bytes move

| Before | Check |
|---|---|
| A log frame is copied | Its checksum chain is valid. A bad frame below the committed end is an error, never treated as the end of the log |
| A database-file page is copied | It matches the checksum remembered from the log frame that last wrote it |
| A file becomes visible in the replica | The staged file decodes and verifies on the same read that uploads it, and its header matches its name. Every S3 upload sends an MD5, and the returned ETag is compared with it |
| Files are merged | Every input verified, and the inputs are contiguous |
| A file is deleted | The replica still restores to the same newest transaction without it |
| A restored database appears | Every file verified before any of its pages counted. The database is built in a scratch file and published by hard link, so the target is absent or complete. `--integrity-check` adds SQLite's own check |

## Design goals and guarantees

What it promises:

- **Never silently wrong.** A request the replica cannot meet exactly is an error, never a shorter or different history.
- **Never harm the host application.** No panic crosses the library boundary. Failures are typed, and memory, queues and retry waits are bounded.
- **Keep backing up.** A persistent failure (bad credentials, a corrupt object, a damaged log) puts one task in a degraded state that health reports, and the rest keeps running. Each cause can be set to stop instead.
- **Stay interchangeable with Go.** No file, header or metadata that Go Litestream does not understand. Switching implementations in either direction is a restart.

What it refuses to do:

- Overwrite or delete a file it does not own. A restore never replaces a database, and never removes a stale `-wal` unless asked to.
- Use bytes it has not verified on the read that produced them.
- Keep writing once it sees another writer on the same replica. Two writers would interleave two histories and delete each other's files, so this is the one failure that stops replication by default.
- Accept a config key it would not honour. A key for a missing feature is an error naming the key, not a setting that quietly does nothing.

What it does not prevent is in [docs/PLAN.md](docs/PLAN.md) section 7. The main item: files written without a running page checksum (Go's default, and so this port's) can only be proven correct by restoring them, so schedule verification.

## Differences from Go Litestream

**Format: the same.** Both write LTX v3 files to the same paths with the same metadata, and use the same staging layout. This port reads everything Go 0.5.0 and later wrote; Go 0.5.16 and later reads everything this port writes, including through Go's read-replica VFS. The two compress pages differently, so files are equal in restored pages, not in bytes.

Behaviour differs where Go's is unsafe or silent. Go references are to upstream commit 041a8c3.

| Area | Go Litestream | litestream-rs |
|---|---|---|
| Deployment | A separate process | A library in the application's process, or a separate process |
| Restore onto an existing file | `-force` overwrites it | Never; there is no such flag |
| Gap in the history | A timestamp restore stops at the gap and exits 0 | An error in every mode |
| Instant inside a merged file | Restores the last file boundary before it | Refuses, unless `--allow-earlier` |
| Corrupt log frame below the committed end | Read as the end of the log | An error (`wal-corrupt`), reported in health |
| Database-file pages | Not checked | Checked against a per-page checksum table before upload |
| Uploads | MD5 depends on provider and config | MD5 always sent, ETag always compared, file verified as it uploads |
| Restart after a crash | Compares the last page and the log's salts | Compares the whole newest staged file with the log, and reads back the newest file of each level |
| A second writer on the replica | No check | Stops (configurable) |
| Persistent failures | Logged, retried | Retried with the same 5 minute cap, and reported as a typed degraded state per task |
| Merging | Unbounded number of inputs, merges across a gap | At most 32 inputs, never across a gap |
| Large-log checkpoint (TRUNCATE) | At 121,359 pages | Only if the application says it holds no long readers (`ReaderTracking::Tracked`, or `app-readers: none`) |
| Unknown config key | Warns and starts | Refuses to start, with a suggestion |
| Config key for a missing feature | n/a | Refused unless at its off value; a knob with no effect here warns |
| Shutdown | Each database drains in turn | One deadline for all databases; SIGHUP also stops cleanly |
| A database that fails to start | The process exits | The others keep running |
| Health | `/metrics` only | `/metrics` and `/healthz` (503 once a database has stopped) |
| HTTP client | AWS SDK | A small signer over `ureq` (blocking, no async runtime) or `reqwest`; TLS is always verified |

Defaults are Go's (1 s polling, checkpoints at 1,000 pages or 1 minute, levels at 30 s, 5 min and 1 h, daily snapshots, 5 minute level 0 retention), except on S3:

| S3 setting | Go | litestream-rs |
|---|---|---|
| Parts of one upload in flight (`concurrency`) | 5 | 5 |
| Same, on Cloudflare R2 | 2 | 2, and a configured value wins |
| Same, on Tigris | 2 | 5 |
| Requests for file creation times in flight | 50 | 16 |
| Restore reads and listing pages in flight | 1 | 8 |

Rust-only settings live under one top-level `litestream-rs:` key in `litestream.yml`, which Go ignores, so one file starts either. `lsr replicate` takes Go's flags as they are; `lsr restore`, `verify` and `files` have their own (`--replica`, `--output`).

**Not built.** Where a config key or flag asks for one of these, the start is refused, never the request ignored:

- Storage other than S3-compatible services and a local directory (no GCS, Azure, SFTP, WebDAV, NATS, OSS).
- The read-replica VFS and `restore -f` (follow). Go's VFS reads replicas this port writes.
- The control socket and its commands, heartbeats, the MCP server, `auto-recover`, `verify-compaction`, watching a directory for new databases.
- Server-side encryption keys and storage classes. TLS verification cannot be turned off: `skip-verify` in a config warns and changes nothing.
- `replicate -force-snapshot` and `-enforce-retention`.
- AWS credentials that need a network call or subprocess: instance and container roles, web identity, SSO, `credential_process`. Static keys from the environment or shared files only.
- The v0.3 replica layout, Go's other subcommands (`databases`, `ltx`, `status`, `reset` and the rest), and the Windows service.

[docs/go-terms.md](docs/go-terms.md) maps this code's terms to Go's, LTX's and SQLite's. [docs/proposals/beyond-go.md](docs/proposals/beyond-go.md) lists improvements parked because they need state Go does not understand.

## Testing and correctness

Most of this repository is tests. Each layer catches what the others cannot:

- **Types first.** States, faults and causes are closed enums matched exhaustively, and invalid values cannot be built. For example a checkpoint needs a proof value that only a completed sync can produce.
- **Deterministic simulation** (`crates/sim`). The engine does no I/O of its own, so a seeded simulator drives it through commits, crashes at any step, store faults, clock jumps and power loss, then checks every restore against a model of what the database really held. Every failure prints a seed that replays it. Fixed seeds run on every commit and a rotating set nightly.
- **Contract oracle** (`crates/spec`). The restore planner's contract is restated as a brute-force checker that shares no code with the planner, and tests and fuzzers judge the planner by it.
- **Real SQLite.** A fault-injecting file layer, killed writers, deleted logs, outside checkpoints, and databases past 1 GiB and 4 GiB.
- **Go compatibility** (`crates/compat`). Pinned Go builds, from 0.5.0 to upstream head: Go writes and Rust restores, the reverse, Go's VFS reads Rust's files, and both take turns on one replica with a kill at a random point. A nightly job covers every 0.5.x release, and a map accounts for every upstream Go test: [docs/upstream-test-map.toml](docs/upstream-test-map.toml).
- **Planted bugs.** Every defect fixed is re-introduced nightly from a ledger, and the tests that caught it must fail again: [docs/planted-bugs.md](docs/planted-bugs.md).
- **Mutation testing.** `cargo-mutants` over the format and engine crates, and over each pull request's changed lines: [docs/mutants.md](docs/mutants.md).
- **Fuzzing.** Seven targets over the decoders, the log reader and the planner; the corpus replays in the normal test run: [docs/fuzzing.md](docs/fuzzing.md).
- **Also nightly:** sanitizers, Miri, thread-schedule exploration, and peak-memory gates.

## Status and limits

- Version 0.0.0. No crate is published and the name is a working one.
- Run against a real bucket (Tigris) for 12 hours without a finding ([docs/spikes.md](docs/spikes.md)). The planned 72 hour soak and 24 hour fuzz run have not been done, and nobody has run it in production.
- Tested on Linux (CI) and macOS only.
- One thread per database, plus one while a scheduled verify runs.
- Handing a replica back to Go: releases through 0.5.18 can replicate uncommitted log frames left by a writer stopped mid-commit. Upstream has fixed it, but no release contains the fix yet.

## Repository

Each crate is `crates/<name>`, package `lsr-<name>`. The entry points are `replica` (the library API) and `cli` (the `lsr` binary).

| Crates | Role |
|---|---|
| `ltx`, `wal` | The LTX file format and the SQLite log reader. Pure, no I/O |
| `core` | The engine: sync, checkpoints, uploads, merging, retention, restore, verify. No I/O of its own |
| `sqlite`, `local`, `store-s3`, `store-fs` | The I/O the engine is given: live SQLite, the restore target, and the two stores |
| `replica`, `config`, `sidecar`, `cli` | The library API, Go's config file, the daemon, the binary |
| `sim`, `spec`, `compat`, `fuzzing`, `heap` | Test-only: simulator, contract oracle, Go harness, fuzz targets, allocation counter |

```bash
just check                              # the gate: format, lints, every test that needs nothing external
cargo nextest run -p lsr-core <name>    # one crate or one test while iterating
```

[AGENTS.md](AGENTS.md) has the rules for working here and every other command, by what it needs (Docker, Go, a real bucket). [docs/PLAN.md](docs/PLAN.md) records the design decisions; `docs/plans/` holds the per-phase plans.

Licensed under Apache-2.0.
