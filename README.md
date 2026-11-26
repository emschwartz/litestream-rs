# litestream-rs (working name)

litestream-rs is an in-process Rust port of [Litestream](https://github.com/benbjohnson/litestream) 0.5.x. It is a library, `lsr-replica`, and a binary, `lsr`, that replicate a WAL-mode SQLite database to S3-compatible storage and restore from it. It writes Litestream's own replica format (the same bucket layout and LTX files), so either program restores what the other wrote. It is not affiliated with or endorsed by the Litestream project.

**Status: unpublished and unproven.** The version is 0.0.0, nothing is on crates.io, the name is a working name, and nobody has run it in production. Read [docs/limitations.md](docs/limitations.md) before you trust it with data.

- **Tested:** restores compared page for page with Go Litestream's for every v0.5 release, on small test databases; a deterministic simulator with injected faults; real SQLite under a power-loss model; and one conformance run each on AWS S3, Cloudflare R2 and Tigris.
- **Not tested:** a production workload, a database of tens of gigabytes with `lsr` as the writer, a long soak on any provider at the current code, and a review by anyone outside the project. The rig that kills and partitions the real binary against an S3 server has made exploring runs only, with no counted run yet.

The code and these documents were written by Claude models under the repository owner's direction ([docs/how-it-was-built.md](docs/how-it-was-built.md)).

## Why it exists

- **In-process.** `lsr-replica` replicates from one thread inside the application: no second process to deploy, supervise and keep in step with the application. The `lsr` binary runs the same engine as a sidecar and takes Go's `litestream.yml` and Go's `replicate` and `restore` arguments, for applications that cannot embed Rust.
- **A typed health handle.** `Replicator::health()` is one lock and no I/O. Every failure that lasts is a named degraded state or stop cause, in the handle, in `/healthz` and in `/metrics`, never only a log line.
- **Bounded memory.** The library's buffers are bounded by named constants, and a test pins each bound and fails when it grows. A restore's memory does not grow with the database or the length of its history. A snapshot's still grows with the number of pages.
- **Exact or refused.** Every file is verified before a restore is put in place. A restore that cannot be exact is an error, never a shorter history. A start that finds files in the bucket its working folder cannot account for stops and asks, where Go carries on.

## Quickstart: the binary

Build it, then replicate a database to a local directory and restore it. Flags go after the command.

```sh
cargo build --release -p lsr-cli --bin lsr
lsr="$PWD/target/release/lsr"
cd "$(mktemp -d)"
sqlite3 app.db "PRAGMA journal_mode=wal; CREATE TABLE t(x); INSERT INTO t VALUES (1);"
"$lsr" replicate -once app.db "file://$PWD/replica"
"$lsr" restore -o restored.db "file://$PWD/replica"
sqlite3 restored.db "SELECT count(*) FROM t;"
```

`-once` syncs, uploads and exits. Without it `lsr replicate` runs until a signal: it polls the WAL every second, uploads each new commit, and on SIGINT or SIGTERM uploads what is left before it exits.

For S3-compatible storage, name the replica in a config file. `lsr` reads Go Litestream's `litestream.yml`:

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: s3://my-bucket/app
      endpoint: https://fly.storage.tigris.dev  # leave out for AWS S3
```

```sh
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
"$lsr" replicate -config litestream.yml
```

[docs/running.md](docs/running.md) covers the config file, credentials, running an application under `-exec`, restore targets, log lines, exit codes and the health and metrics endpoints.

## Quickstart: the library

The crates are not published, so depend on the repository:

```toml
[dependencies]
lsr-replica = { git = "https://github.com/emschwartz/litestream-rs" }
lsr-store-s3 = { git = "https://github.com/emschwartz/litestream-rs" }
```

```rust
use std::{path::Path, sync::Arc, time::Duration};

use lsr_replica::{
    Config, IfMissing, Readiness, Replicator, StartOptions, StopOutcome, restore_if_missing,
};
use lsr_store_s3::{BucketName, CredentialChain, Endpoint, Prefix, S3Config, S3Store};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The replica is a bucket and a prefix. The keys come from AWS_ACCESS_KEY_ID and
    // AWS_SECRET_ACCESS_KEY, or from the AWS shared files.
    let config = S3Config::new(
        Endpoint::new("https://s3.us-east-1.amazonaws.com")?,
        BucketName::new("my-bucket")?,
        Prefix::new("app")?,
    );
    let keys = CredentialChain::from_process().resolve()?;
    let store = Arc::new(S3Store::resolved(config, &keys)?);
    let db = Path::new("app.db");
    let replication = Config::default();

    // On a new machine, restore the newest backup; on a first deploy there is none.
    let first_start = restore_if_missing(&*store, db, &replication)? == IfMissing::EmptyReplica;

    // Start before the application opens its own connections.
    let options = StartOptions {
        config: replication,
        create_if_missing: first_start,
        ..StartOptions::default()
    };
    let replicator = Replicator::start(db, store, options)?;
    match replicator.ready(Duration::from_secs(30)).wait() {
        Readiness::Ready => {}
        other => return Err(format!("replication did not start: {other:?}").into()),
    }

    // The application runs here. replicator.health() says where replication stands.

    // At shutdown, after the application's connections close.
    match replicator.stop(Duration::from_secs(30)).wait() {
        StopOutcome::Drained { .. } => Ok(()),
        other => Err(format!("not everything reached the replica: {other:?}").into()),
    }
}
```

No async runtime is needed, and nothing runs on the application's threads. Writes are refused with SQLite's "database is locked" until `ready` answers `Readiness::Ready` (see `if-replication-stops` in [docs/how-it-works.md](docs/how-it-works.md)). The crate documentation of `lsr-replica` covers connection setup (`app_connection`, `connection_setup`), events, restore and the memory bounds.

TLS to the bucket runs on the rustls crypto provider the application chooses: the one it installed with `rustls::crypto::CryptoProvider::install_default`, else the one provider rustls's own features enable in its build (ring or aws-lc-rs). `lsr-store-s3` enables neither, so it links no provider itself. When rustls cannot pick, building the store returns `ConfigError::NoTlsProvider`. The `lsr` binary installs aws-lc-rs (a C build, so it needs a C compiler) at the start of main.

The library and the application share one SQLite, because `libsqlite3-sys` allows one copy per build. `lsr-replica` accepts `libsqlite3-sys` 0.37 to 0.40 (SQLite 3.51.3 to 3.54.1), so an application on rusqlite 0.39 to 0.43 or sqlx 0.9 builds beside it. With `default-features = false` it links the application's own SQLite, which must be one of the verified builds (`crates/sqlite/src/version.rs`) or `Replicator::start` refuses it.

## How it differs from Go Litestream

- **The replica is the same.** Either implementation restores the other's files (Go from v0.5.16 for files written here). Either takes over the other's bucket and working folder, but only once the other has fully stopped: both running on one bucket path, even for seconds, can corrupt the replica ([#346](https://github.com/emschwartz/litestream-rs/issues/346)).
- **It stops where Go carries on.** A start that finds files in the bucket this database cannot account for stops with `other_writer` until an operator chooses the database or the bucket. With the working folder lost it judges by comparing pages and the replicator's own counter, which stops an older copy of the database except in one short window ([limitations](docs/limitations.md)).
- **It can hold the application back.** The `if-replication-stops` setting decides whether a replicator that cannot start, or has stopped, blocks the application's writes. The default holds them at start only.
- **Restores are checked.** Every file is verified before use, and a restore the replica cannot give exactly fails with `Gap`. Go's restore does not check by default.
- **The WAL is synced before it is copied**, so a power loss does not leave the bucket ahead of the database on an honest disk.
- **The config is strict.** An unknown key, a key set twice or a feature that is not built is refused at load, naming the key and line.
- **Not built:** storage other than S3 and a local directory, the VFS read replica, the control socket and its commands, encryption settings, and a few credential sources.

The full list is [docs/differences-from-go.md](docs/differences-from-go.md).

## Known limitations

- Nobody runs it in production, and no long soak has run at the current code.
- Go Litestream and `lsr` writing one bucket path at the same time can leave restores that are wrong with no error ([#346](https://github.com/emschwartz/litestream-rs/issues/346)).
- A disk that loses writes it acknowledged, followed by an application that redoes the same last write, can leave a replica whose history differs from the database's, with no report ([#12](https://github.com/emschwartz/litestream-rs/issues/12)).
- Replication pauses while the database is copied whole: three passes at a first start ([#340](https://github.com/emschwartz/litestream-rs/issues/340)) and one for each snapshot, daily by default. Go keeps syncing through its snapshots.
- It needs two S3 permissions that the policy in Litestream's guide does not grant, and free disk about the size of the database at a first start ([docs/running.md](docs/running.md)).
- Memory during a snapshot grows with the page count ([#64](https://github.com/emschwartz/litestream-rs/issues/64)), and a replica's whole listing is held in memory ([#68](https://github.com/emschwartz/litestream-rs/issues/68)). A snapshot's read load on the application's disk is not limited ([#315](https://github.com/emschwartz/litestream-rs/issues/315)).
- A filesystem where SQLite's locks or shared memory do not work (NFS, some container file sharing) is not refused ([#332](https://github.com/emschwartz/litestream-rs/issues/332)).
- It is built and tested on Linux and macOS only. Assumed-role and SSO credentials are not built ([#176](https://github.com/emschwartz/litestream-rs/issues/176)).

The full list, grouped by kind, is [docs/limitations.md](docs/limitations.md).

## How it was built

Claude Code agents wrote the code, directed by the repository's owner. What they had to meet is a contract, not Go's behaviour: the replica format, and equality with Go on restored database pages. The evidence is layered: differential tests against pinned Go binaries, a deterministic simulator with fault injection and judges that share no code with the engine, real-SQLite crash and power-loss rigs, fuzzing, a ledger of planted bugs that the tests must catch every night, and a behaviour table that names the test behind each claim. [docs/how-it-was-built.md](docs/how-it-was-built.md) says what that method caught and what it has not proven.

## Where the rest lives

- [docs/how-it-works.md](docs/how-it-works.md): the mechanism on one page.
- [docs/running.md](docs/running.md): running the binary.
- [docs/differences-from-go.md](docs/differences-from-go.md) and [docs/go-terms.md](docs/go-terms.md): differences from Go, and a vocabulary map.
- [docs/limitations.md](docs/limitations.md) and [docs/release-bar.md](docs/release-bar.md): what is not done, and what must be true before first use.
- [docs/BEHAVIOR.md](docs/BEHAVIOR.md): every circumstance the code distinguishes, what it does, and what a test has shown.
- [docs/config-keys.md](docs/config-keys.md): every `litestream.yml` key and what it sets.
- [docs/PLAN.md](docs/PLAN.md) and `docs/decisions/`: the design, the crate map (section 3) and each trade-off, one file per decision.
- [AGENTS.md](AGENTS.md): the rules for working in this repository.

## Developing

Each crate is `crates/<name>`, package `lsr-<name>`. `just check` is the land gate. Tests by what they need:

| Needs | Command | What runs |
|---|---|---|
| Nothing | `just check` | Format, clippy and every test that is not ignored over all features, doctests, the S3 client builds with each HTTP client alone, and the test map's counterparts against a fresh test list (Linux). The land gate. |
| Nothing | `just shuttle` | Schedule exploration of the replica thread and the S3 batch under a `--cfg shuttle` build of its own. Not in `just check`; CI runs it on pull requests that touch `crates/store-s3` or `crates/replica`, and nightly runs far more schedules. |
| Nothing, minutes | `just sqlite-big`, `just mem-big`, `just sim-soak <secs>`, `just sim-nightly`, `just fuzz-seed`, `just mutants <crate>` | The large real-SQLite cases (several GB of disk), the memory gates at 1 to 4 GiB and a million files, rotating simulator seeds, the fuzz seed corpus, mutation testing. |
| Docker | `just test-s3`, `just compat-s3` | The S3 store, `lsr` and the history judge's real-engine run (and Go and Rust together) against a throwaway MinIO. Fails when Docker is down; `LSR_SKIP=minio just test-s3` opts out and runs nothing. `just test-s3` also needs Go, to build the pinned Toxiproxy (MIT licence) that the history judge's network-fault tests put between the engine and MinIO; `LSR_SKIP=toxiproxy` opts out of those. |
| Docker, Go | `just fault-rig <cycles>`, `just fault-rig-canary`, `just fault-rig-plant <name>` | The real `lsr replicate` beside a SQLite writer with its replica on MinIO behind Toxiproxy, under a seeded menu (kill -9, partitions, slow and cut transfers, the pinned Go Litestream on the same prefix; LazyFS power loss and a libfaketime clock step on Linux), judged by point-in-time restores (`scripts/fault_rig.py`, `docs/decisions/D195.md`). One counted run is recorded: nightly run 45565674128 on 2026-11-25 (seed 1688438391, 1,000 cycles in 123 segments, 7,399 s, LazyFS and libfaketime on, 128,316 commits, 14,369 restores, 0 failing anomalies, 151 known anomalies under #346; [D216](docs/decisions/D216.md)). One seed, one run. Exit 1 on an anomaly, whose evidence is kept under `target/fault-rig/evidence`. |
| Docker, Go | `just trial-drill <gib>`, `just trial-drill-small`, `just trial-drill-canary` | The trial rehearsal at size: Go replicates a synthetic database of the given size, lsr takes over under load (kill -9 during a large upload, a compaction and a snapshot), both restore and are compared with a ledger of commits, and Go takes it back (`scripts/trial_drill.py`, `docs/trial-drill.md`). Nightly runs it at 0.05 GiB. A real provider: `just trial-drill-provider tigris <gib>`. |
| Go | `just upstream`, then `just compat`, `just compat-live <names>`, `just compat-releases`, `just compat-tags`, `just compat-regen` | Pinned Go builds from `.upstream/` (a worktree shares the main checkout's): Go writes and Rust restores, the reverse, handovers and alternation, the Go VFS (which also needs a C compiler and SQLite headers), and the effective-config differentials against a dumper built from the pinned Go source (`crates/compat/src/dump.rs`). `compat-tags` needs only the clone. |
| A real provider (Tigris, AWS S3, Cloudflare R2) | `just test-provider <tigris\|aws\|r2>`, `just test-tigris`, `just tigris-restore`, `just soak-tigris <hours>` | The real bucket, with credentials from `~/.config/litestream-rs/<provider>.env` (`scripts/provider.sh`; Tigris also takes `AWS_*`). `test-provider` runs the conformance suite, the create-only write and race checks, consistency, error classification, abandoned uploads and one `lsr replicate` and restore, under a run prefix it empties afterwards; `test-tigris` is its Tigris case. `tigris-restore` also builds Go v0.5.18. Never in CI. |
| Nightly Rust | `just fuzz <target> <secs>`, `just fuzz-cmin <target>` | cargo-fuzz over `fuzz/`. |

The gate is dominated by compiling, so a compiler cache helps on a machine that builds many worktrees: `cargo install sccache --locked`, then `export RUSTC_WRAPPER=sccache SCCACHE_DIR=~/.cache/sccache` (one directory shared by every checkout) before `just check`. It is optional; nothing in the repo requires it.

A test that needs something outside the process (MinIO, the Go builds, Tigris credentials, a C compiler, free disk) fails when it is missing; it never passes by skipping. The one opt-out is the runner naming it: `LSR_SKIP=minio` (comma separated: `minio`, `toxiproxy`, `go`, `tigris`, `aws`, `r2`, `cc`, `disk`). CI never sets it, and under CI (the `CI` variable) setting it is itself a failure. The rule lives in `lsr_core::needs`. A test that hangs is killed by nextest (`.config/nextest.toml`) and CI jobs carry `timeout-minutes`, so a hang is a failure, not a six-hour job.