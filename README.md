# litestream-rs (working name)

litestream-rs is an in-process Rust port of [Litestream](https://github.com/benbjohnson/litestream) 0.5.x: a library, `lsr-replica`, and a drop-in binary, `lsr`. Both replicate a WAL-mode SQLite database to S3-compatible storage and restore it. They write Litestream's own replica format, so Go Litestream restores what `lsr` wrote and the reverse. The project is not affiliated with or endorsed by the Litestream project.

**Status: unpublished and unproven.** The version is 0.0.0, nothing is on crates.io, and nobody runs it in production. Read [docs/limitations.md](docs/limitations.md) before you trust it with data; [docs/release-bar.md](docs/release-bar.md) says what must be true before first use. Never run Go Litestream and `lsr` on one bucket path at once, even for seconds: it can leave restores that are wrong with no error ([#346](https://github.com/emschwartz/litestream-rs/issues/346)).

Claude models wrote the code and these documents under the repository owner's direction; [docs/how-it-was-built.md](docs/how-it-was-built.md) says how it was checked.

## Why use it

- **In-process.** The library replicates from a thread it starts inside the application, so there is no second process to deploy and supervise. The binary runs the same engine as a sidecar.
- **Tested against Go.** A nightly job compares restores page for page with Go Litestream's, for each of the 19 releases from v0.5.0 to v0.5.18. Three more checks: a deterministic fault simulator, real SQLite under a power-loss model, and a rig that kills and partitions the real binary (one counted run: 1,000 cycles, 0 failing anomalies, 151 excused under [#346](https://github.com/emschwartz/litestream-rs/issues/346)).
- **Visible failures.** `Replicator::health()` takes one lock and does no I/O. Every failure that lasts is a named state in the handle, in `/healthz` and in `/metrics`, never only a log line.
- **Faster, with less memory and fewer requests.** On one 4-core Linux VM against MinIO on loopback, a first start of a 4 GiB database of random pages, at the default part settings ([docs/performance.md](docs/performance.md)):
  - **Faster:** 34.6 s against Go's 59.6 s (three paired runs).
  - **Less memory:** a peak of 55 MiB against Go's 171 MiB (at a large part size see [docs/performance.md](docs/performance.md)).
  - **Fewer requests:** 1,680 against Go's 3,809 in one counted run, though 40 LISTs against Go's 33 or 34.
- **No silent short restores.** A restore verifies every file before it uses it, and fails rather than hand back a shorter history.

## Run the binary

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

`-once` replicates one pass and exits. Without it, `lsr replicate` polls the WAL every second and uploads each new commit until SIGINT or SIGTERM, then uploads what is left and exits.

For S3-compatible storage, name the replica in a config file:

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

`lsr` reads Go's `litestream.yml` and takes Go's `replicate` and `restore` arguments, so it can replace the `litestream` binary in place. [docs/running.md](docs/running.md) covers the config file, credentials, `-exec`, restore, logs, exit codes, the health and metrics endpoints, and moving from Go and back.

## Embed the library

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

No async runtime is needed, and nothing runs on the application's threads. Until the replicator reports that it is ready, writes from any connection fail with "database is locked". The `lsr-replica` crate docs (`cargo doc -p lsr-replica --open`) cover connections, the TLS provider, which SQLite builds it accepts, restore and the memory bounds.

## How it differs from Go

The replica format is the same. Where Go would carry on, `lsr` stops or refuses: a bucket holding files the database cannot account for, a restore that cannot be exact, an unknown config key. The full list is in [docs/differences-from-go.md](docs/differences-from-go.md).

## Where the rest lives

- [docs/how-it-works.md](docs/how-it-works.md): the mechanism on one page.
- [docs/running.md](docs/running.md) and [docs/config-keys.md](docs/config-keys.md): running the binary, and every `litestream.yml` key.
- [docs/differences-from-go.md](docs/differences-from-go.md) and [docs/go-terms.md](docs/go-terms.md): differences from Go, and a vocabulary map.
- [docs/limitations.md](docs/limitations.md) and [docs/release-bar.md](docs/release-bar.md): what is not done, and what must be true before first use.
- [docs/performance.md](docs/performance.md): measurements against Go and how they were taken.
- [docs/BEHAVIOR.md](docs/BEHAVIOR.md): every circumstance the code distinguishes, what it does, and which test shows it.
- [docs/PLAN.md](docs/PLAN.md) and `docs/decisions/`: the design, the crate map (section 3) and one file per trade-off.
- [docs/developing.md](docs/developing.md): the land gate (`just check`) and which tests need Docker, Go or a real provider.
- [AGENTS.md](AGENTS.md): the rules for working in this repository.
