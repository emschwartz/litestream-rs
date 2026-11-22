# litestream-rs (working name)

A Rust port of [Litestream](https://github.com/benbjohnson/litestream) 0.5.x. It continuously copies a live SQLite database to S3-compatible storage (tested on MinIO and Tigris) or to a directory, and restores it to the latest state, a transaction, or a point in time.

It runs as a library inside a Rust application, or as a sidecar that reads Go's `litestream.yml` and refuses to start when the config asks for a feature it lacks. Both write the same files as Go Litestream: this port reads replicas written by Go 0.5.0 and later, and Go 0.5.16 and later reads replicas this port writes.

It aims for two things at once: compatibility with Go Litestream (same files, same config, either can take over a replica the other wrote), and correctness beyond what Go checks. Where Go would continue from a state it cannot prove, this port refuses or takes a fresh snapshot instead. [Differences from Go Litestream](#differences-from-go-litestream) lists where that changes behaviour.

Not affiliated with or endorsed by the Litestream project.

**Status.** Early and not proven in production. The code was written by AI agents. The maintainer directs the design and the testing strategy but does not review the code line by line, so confidence has to come from testing, and the testing is built to check the system from the outside rather than trust its own view of itself.

Open problems are listed under [Known limits](#known-limits).

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

# Restore. The output must not exist: a restore refuses an existing file.
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

// A restore is a free function. It builds a new file and refuses an existing target.
restore(&*store, RestoreTarget::Latest, &restored)?;
```

For S3 the only change is the store: `Arc::new(lsr_store_s3::S3Store::new(config, credentials)?)`. `Config::default()` is Go Litestream's defaults. `replicator.health()` returns a copy of the replicator's state to alert on: what is replicated, how far behind, and any failure it keeps retrying.

The embedding application must:

- Start the replicator before opening any other connection, and pass every connection through `app_connection` (or `app_connection_raw` if it is not a `rusqlite` connection).
- Run no checkpoints of its own, and never open and close the database file with plain file I/O. Closing any file handle on a database drops every lock SQLite holds on it in that process.
- Call `stop` at shutdown, after its writers close.
- Build against the same bundled SQLite. `lsr-sqlite` uses `rusqlite` 0.43 with SQLite compiled in, and Cargo allows one SQLite per build.

## How it works

SQLite in WAL mode appends every change to a write-ahead log (the `-wal` file) and later copies it into the database file, a step called a checkpoint. Replication reads the log before that happens.

```
application ──writes──▶ app.db-wal ──poll 1 s, fsync, verify──▶ .app.db-litestream/ ──verify, upload──▶ level 0
                                                                (staging directory)                       │ merged every
                                                                                                          ▼ 30 s, 5 min, 1 h
                        app.db ─────────────────── every 24 h, all pages ─────────────────────▶ level 9   levels 1, 2, 3
```

**Capture.** The replicator holds a read transaction for its whole life, which stops SQLite from recycling log entries it has not copied yet. Every second it reads the new committed transactions, checks each log frame's checksum, and writes them as one file. It runs the checkpoints itself, after it has copied everything up to that point.

**WAL fsync.** Before it stages anything, the replicator fsyncs the WAL. Under `synchronous=NORMAL` SQLite leaves committed frames unsynced until its next checkpoint, so without this step a power cut could leave the replica holding commits the database lost. If the fsync fails, nothing is staged, and health shows a failure that it keeps retrying.

**LTX files and levels.** Each file is in LTX, Litestream's format: a set of changed pages covering a range of transaction ids, compressed and checksummed. Level 0 holds one small file per sync, and levels 1 to 3 hold merges of the level below over longer windows, so a restore reads a few large files instead of thousands of small ones. Level 9 holds snapshots: every page of the database.

**Staging directory.** New files land first in a directory beside the database (`.<name>-litestream`, Go's "meta directory"), are synced to disk, and are then uploaded in transaction order. A file is uploaded only if the running process wrote it, or if a restart re-derived it from the live WAL. Staged files a restart cannot re-derive are deleted, then staged again from the WAL when it still continues from the last proven file, or else replaced by a snapshot.

**Snapshots.** A full copy is taken on the first sync, every 24 hours by default, and whenever the replicator cannot prove the log continues what it already copied. Each such case has a named cause in health and events, for example the log was deleted or another process checkpointed past the replicator. The cost is a full read and upload of the database, and a process that restarts in a loop pays it repeatedly (see [Known limits](#known-limits)).

**Compaction and retention.** A merge takes at most 32 contiguous files and does not delete its inputs. Retention deletes separately: snapshots older than the retention period (24 hours by default), the merged files only those snapshots needed, and level 0 files older than 5 minutes that a merged file covers. When a listing cannot be read as one history, retention deletes nothing.

**Restore planning.** A restore lists the replica once and plans a chain: the file that starts at transaction 1 and reaches furthest, then each file that continues it. The target is the latest state, a transaction id (`--txid`), or an instant (`--timestamp`). If the replica cannot produce exactly that state the restore fails and says why: a gap in the history, a target past the newest file, or no starting file.

**Point in time.** A merged file carries only its newest input's time. When an instant falls inside a merged file whose inputs retention already removed, the exact state is not recoverable. The restore refuses, unless `--allow-earlier` accepts the newest state provably before the instant.

**Verification.** `lsr verify` decodes every file, checks its checksums and that its contents match its name, reports every gap, and then runs a real restore of the latest state into a sink that writes nothing. The library can do the same on a schedule (`validation.interval`, off by default as in Go) and compares the result with the live database.

### Checks on each path

| Before | Check |
|---|---|
| A log frame is copied | Its checksum chain is valid. A bad frame below the committed end is an error, not the end of the log |
| A database-file page is copied | It matches the checksum remembered from the log frame that last wrote it |
| A file becomes visible in the replica | The staged file decodes and its checksum verifies on the same read that uploads it, its header matches its name, and its bytes match the proof token described below |
| An S3 request writes bytes | Each PUT and each part of a multipart upload sends Content-MD5, and a returned ETag in MD5 form must equal it. A completed multipart object's ETag is checked only for its shape and part count |
| Files are merged | Every input decoded and verified, and the inputs are contiguous |
| A file is deleted | Judged from a listing, the replica still restores to the same newest transaction without it. A listing that lags the bucket can mislead this check (see [Known limits](#known-limits)) |
| A restored database appears | Every file verified before any of its pages counted. The database is built in a scratch file, fsynced, and published by hard link, so on a filesystem with hard links the target is absent or complete. `--integrity-check` adds SQLite's own check |

The LTX file checksum covers decompressed pages, not the compressed bytes, and the default format (Go's) carries no running checksum across files. Both limits are under [Known limits](#known-limits).

## Design goals

These are goals, each with the mechanism meant to deliver it. None is a guarantee at version 0.0.0.

| Goal | Mechanism |
|---|---|
| No silent wrong answer | A restore that cannot produce exactly the requested state fails with the reason. A config key the port would not honour is an error naming the key. This is the property most of the testing below checks, and the open defects under Known limits are cases where it does not hold yet |
| Contain failures in the host application | The replicator thread catches panics at its boundary. `unsafe` is denied outside the SQLite crate and a test-only allocator. Retry waits are capped, and nightly tests gate peak memory. Not contained: the replicator holds a read transaction, so if it stalls the WAL grows |
| Keep backing up | A persistent failure (bad credentials, a corrupt object, a damaged log) puts one task in a degraded state that health reports, and the rest keeps running. Each cause can be set to stop instead. Alert on `health()` or `/healthz`: a degraded task nobody watches is a backup gap |
| Do not damage what it does not own | A restore builds a new file and refuses an existing target. On seeing another writer's files in the replica, replication stops by default, because two writers would interleave two histories |
| Stay interchangeable with Go | No file, header or metadata that Go Litestream does not read. Tested against Go 0.5.0 to upstream head. One hazard when handing a replica back to Go is under Known limits |

## Correct by construction

Some classes of bug are made hard to write, before any test runs:

- **Uploads need a proof token.** The uploader takes an in-memory token that carries a staged file's key, length, checksum and a digest of its raw bytes, and compares it with the bytes it reads for the upload. A token is minted in two places only: when the running process stages the file from the WAL, and at boot when the file is re-derived from the live WAL. A staging directory restored from a backup is therefore not uploaded on the strength of its file names.
- **Boot reads the committed WAL end once.** A restart takes one read of where SQLite says the committed log ends, and every boot check reads through that single bound. Frames past it are not used, even when their checksums chain.
- **The WAL is fsynced before anything is staged.** The replica should not run ahead of the local database, unless the disk lies about fsync.
- **Typed failure causes.** States, faults and causes are closed enums matched exhaustively, so a new failure cause does not compile until every place that must handle it does.
- **Invariants report, they do not panic.** In a release build a broken internal invariant becomes a typed fault that rebuilds the engine and shows in health, instead of a panic in the host application. Debug and test builds panic.

These remove classes of bugs. They do not show the code is right, which is what the next section is for.

## How it is tested

Each row is a class of bug, the strategy aimed at it, and what that strategy cannot catch. Tests check, they do not prove.

| Bug class | Strategy | What it cannot catch |
|---|---|---|
| Wrong decisions across crashes, restarts and store faults | Deterministic simulation with fault injection (`crates/sim`). The engine does no I/O of its own, so a seeded simulator drives it through commits, process kills, power loss, store errors and clock jumps, then judges every restore. A failure prints a seed that replays it | A wrong belief about SQLite or the disk that the engine and the simulator share. It samples histories and does not cover all of them |
| Restores that are not a committed state | An independent restore checker (`crates/history`). It depends on no engine crate and never sees a transaction id or WAL offset. The workload writes a hash-chained table, so a restored file certifies itself, and the checker judges restored content against a log of what the application saw commit | Damage in pages the workload never wrote. On LazyFS only restores of the latest state are judged |
| Disk behaviour in a crash | A block-level crash model: each block written since the last fsync keeps its old or new bytes independently, and real SQLite recovers every resulting image. Also LazyFS runs of the real binary: 1000 power-loss cycles with 0 anomalies, which bounds the per-cycle anomaly rate at about 0.3% or less (95% confidence), for that fault mix only | Crash behaviour outside the model. LazyFS does not model a lost directory entry, and a planted bug that skips the fsync before a staging rename went unnoticed by the checker in 200 cycles |
| Wrong restore plans | An independent contract spec (`crates/spec`): the planner's contract restated as a brute-force checker with no dependencies. Tests and two fuzz targets judge the planner by it | A mistake in the contract itself |
| Corrupt files | Fuzzing of the LTX decoder, page index, key parser, WAL reader and planner. A 24-hour run of those five targets found no crash, hang or out-of-memory. The corpus replays on every commit | A well-formed file with wrong content. The two newer planner targets have not had a 24-hour run |
| Go incompatibility | Tests against real Go builds: every 0.5.x release and upstream head, nightly. Go writes and Rust restores, the reverse, handovers both ways, and from 0.5.16 both alternate on one replica with a kill at a random point | A bug both implementations share |
| Fixed bugs returning | A ledger of planted bugs: each fixed defect is kept as a patch and re-planted nightly, and the tests that caught it must fail again. Each pull request also runs mutation testing on its changed lines in the engine and format crates | Bugs nobody has found yet. It shows the regression tests still bite, nothing more |
| Tests that do not test | A test that needs an outside service fails without it and never skips, and a test that walks files or fixtures asserts it found some. Canaries for the runner, required coverage states for the simulator, and blind-seeded bugs that measure each layer's recall are not merged yet ([#86](https://github.com/emschwartz/litestream-rs/pull/86) holds the first two) | Until those land, nothing measures how many unknown bugs the layers above would find |

Sanitizers, Miri, thread-schedule exploration and peak-memory gates also run nightly. [docs/TESTING.md](docs/TESTING.md) has every recipe.

## Known limits

Open problems on `main` today:

- **Crash loops grow the disk and the bucket.** A process killed every few seconds snapshots on many boots and does not get to run retention, and a killed upload can leave a temp file behind. One run of about 470 boots took level 0 to 4.1 GB and staging to 2.8 GB. A fix is in progress and not merged.
- **Some single-byte changes in compressed data still verify.** The LTX file checksum does not cover LZ4 framing bytes, and the decoder accepts a few changed bytes it could refuse ([#95](https://github.com/emschwartz/litestream-rs/pull/95), open).
- **A lagging bucket listing is trusted.** In the simulator, when a listing trails the bucket, the replicator can stop on a second writer that is not there, report a replica regression that did not happen, or reuse a freed key so that one transaction id gets two contents ([#96](https://github.com/emschwartz/litestream-rs/pull/96), open).

What the testing does not establish:

- In the first measurement, the simulator alone, with no test written for the bug, found 7 of 22 planted bugs. Most of the rest could not be measured in that run.
- The crash model is assumed from the ALICE literature (Pillai et al., OSDI 2014). It has not been observed on real disks.
- MinIO (in CI) and Tigris (by hand, including one 12-hour soak from one machine) are exercised. AWS S3 and Cloudflare R2 are not.
- Results of the blind-seeding recall study are pending. They will be reported here whatever they are.
- Nobody outside the project has reviewed or audited it.

Limits of the design:

- **No page checksums by default.** Files carry no running page checksum, as in Go's default, so a stale but well-formed page passes every file-level check. A restore compared with the live database is the real proof. Scheduled verification (`validation.interval`) is off by default: turn it on, or run `lsr verify` and a test restore on a schedule.
- **Handing a replica back to Go.** Go releases through 0.5.18 can replicate uncommitted log frames left by a writer stopped mid-commit. Upstream has fixed it, but no release contains the fix yet.
- A stray process that checkpoints or deletes the WAL (`sqlite3`, another tool) is detected and costs a snapshot.
- The library shares the application's fate: an out-of-memory kill stops replication until restart.
- Version 0.0.0. No crate is published and the name is a working one. Tested on Linux (CI) and macOS (nightly) only. One thread per database, plus one while a scheduled verify runs.

## Differences from Go Litestream

**Format: the same.** Both write LTX v3 files to the same paths with the same metadata, and use the same staging layout. This port reads everything Go 0.5.0 and later wrote; Go 0.5.16 and later reads everything this port writes, including through Go's read-replica VFS. The two compress pages differently, so files are equal in restored pages, not in bytes.

Behaviour differs in the cases below. The Go column comes from reading upstream's source at commit 041a8c3; not every Go row was reproduced by a test.

| Area | Go Litestream | litestream-rs |
|---|---|---|
| Deployment | A separate process | A library in the application's process, or a separate process |
| WAL durability before upload | Reads and uploads WAL frames without syncing the WAL | Fsyncs the WAL before staging frames read from it |
| Restore onto an existing file | `-force` overwrites it | Refuses; there is no such flag |
| Gap in the history | A timestamp restore stops at the gap and exits 0 | An error in every mode |
| Instant inside a merged file | Restores the last file boundary before it | Refuses, unless `--allow-earlier` |
| Corrupt log frame below the committed end | Read as the end of the log | An error (`wal-corrupt`), reported in health |
| Database-file pages | Not checked | Checked against a per-page checksum table before upload |
| Uploads | MD5 depends on provider and config | Content-MD5 on every PUT and part, ETag compared when it is an MD5, file verified as it uploads |
| Restart after a crash | Compares the last page and the log's salts | Reads SQLite's committed WAL end once, and compares the whole newest staged file with the WAL range it covers |
| Staged files found at a restart | Uploaded without a check against the WAL | Uploaded only when re-derived from the live WAL; otherwise deleted, then staged again or replaced by a snapshot (`staged_unverified`) |
| A second writer on the replica | No check | Stops (configurable) |
| Persistent failures | Logged, retried | Retried with the same 5 minute cap, and reported as a typed degraded state per task |
| Merging | Unbounded number of inputs; a gap in a level fails its merges | At most 32 inputs; a gap ends one merge and the next starts after it |
| Large-log checkpoint (TRUNCATE) | At 121,359 pages | Only if the application says it holds no long readers (`ReaderTracking::Tracked`, or `app-readers: none`) |
| Unknown config key | Warns and starts | Refuses to start, with a suggestion |
| Config key for a missing feature | n/a | Refused unless at its off value; a knob with no effect here warns |
| Shutdown | Each database drains in turn | One deadline for all databases; SIGHUP also stops cleanly |
| A database that fails to start | The process exits | The others keep running |
| Health | `/metrics` only | `/metrics` and `/healthz` (503 once a database has stopped) |
| HTTP client | AWS SDK | A small signer over `ureq` (blocking, no async runtime) or `reqwest`; TLS is always verified |

Defaults are Go's (1 s polling, checkpoints at 1,000 pages or 1 minute, levels at 30 s, 5 min and 1 h, daily snapshots, 5 minute level 0 retention), except on S3. The R2 row is a default only: R2 has not been run against.

| S3 setting | Go | litestream-rs |
|---|---|---|
| Parts of one upload in flight (`concurrency`) | 5 | 5 |
| Same, on Cloudflare R2 | 2 | 2, and a configured value wins |
| Same, on Tigris | 2 | 5 |
| Requests for file creation times in flight | 50 | 16 |
| Restore reads in flight | Opens up to 32 files at once | 8 ranged reads |

Rust-only settings live under one top-level `litestream-rs:` key in `litestream.yml`, which Go ignores, so one file starts either. `lsr replicate` takes Go's flags as they are; `lsr restore`, `verify` and `files` have their own (`--replica`, `--output`).

**Not built.** Where a config key or flag asks for one of these, the start is refused, not the request ignored:

- Storage other than S3-compatible services and a local directory (no GCS, Azure, SFTP, WebDAV, NATS, OSS).
- The read-replica VFS and `restore -f` (follow). Go's VFS reads replicas this port writes.
- The control socket and its commands, heartbeats, the MCP server, `auto-recover`, `verify-compaction`, watching a directory for new databases.
- Server-side encryption keys and storage classes. TLS verification cannot be turned off: `skip-verify` in a config warns and changes nothing.
- `replicate -force-snapshot` and `-enforce-retention`.
- AWS credentials that need a network call or subprocess: instance and container roles, web identity, SSO, `credential_process`. Static keys from the environment or shared files only.
- The v0.3 replica layout, Go's other subcommands (`databases`, `ltx`, `status`, `reset` and the rest), and the Windows service.

[docs/go-terms.md](docs/go-terms.md) maps this code's terms to Go's, LTX's and SQLite's. [docs/proposals/beyond-go.md](docs/proposals/beyond-go.md) lists improvements parked because they need state Go does not understand.

## Repository

Each crate is `crates/<name>`, package `lsr-<name>`. The entry points are `replica` (the library API) and `cli` (the `lsr` binary).

| Crates | Role |
|---|---|
| `ltx`, `wal` | The LTX file format and the SQLite log reader. Pure, no I/O |
| `core` | The engine: sync, checkpoints, uploads, merging, retention, restore, verify. No I/O of its own |
| `sqlite`, `local`, `store-s3`, `store-fs` | The I/O the engine is given: live SQLite, the restore target, and the two stores |
| `replica`, `config`, `sidecar`, `cli` | The library API, Go's config file, the daemon, the binary |
| `sim`, `spec`, `history`, `compat`, `fuzzing`, `heap` | Test-only: simulator, contract spec, restore checker, Go harness, fuzz targets, allocation counter |

```bash
just check                              # the gate: format, lints, every test that needs nothing external
cargo nextest run -p lsr-core <name>    # one crate or one test while iterating
```

[AGENTS.md](AGENTS.md) has the rules for working here. [docs/TESTING.md](docs/TESTING.md) has every other command, by what it needs (Docker, Go, FUSE, a real bucket). [docs/PLAN.md](docs/PLAN.md) records the design decisions; `docs/plans/` holds the per-phase plans.

Licensed under Apache-2.0.
