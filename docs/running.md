# Running lsr

`lsr` is the binary: a sidecar that replicates one or more SQLite databases, plus the commands to restore and inspect a replica. It takes Go Litestream's `litestream.yml` and Go's `replicate` and `restore` arguments. The [README](../README.md) has the shortest working example; this page has the rest.

Build it with `cargo build --release -p lsr-cli --bin lsr`; the binary is `target/release/lsr`. `lsr --help` lists the commands.

## Commands

| Command | What it does |
|---|---|
| `lsr replicate` | Replicates every database in the config, or one database to one replica URL, until a signal |
| `lsr restore` | Rebuilds a database from a replica |
| `lsr verify` | Checks a replica for damage and gaps without restoring |
| `lsr files` | Lists a replica's files with sizes and times |
| `lsr trust-local` | Tells a stopped replicator that the local database is the right one |
| `lsr config check` | Reads a config as a start would and prints every refusal and warning |
| `lsr databases`, `lsr ltx`, `lsr wal` | Go's listing commands |

Flags go after the command: `lsr -config f replicate` is a usage error. As in Go, the flags of `replicate` and `restore` come before the database path and the replica URL.

## The config file

`lsr replicate` reads `-config <file>`, else `$LITESTREAM_CONFIG`, else `/etc/litestream.yml`. `${VAR}` in the file is expanded from the environment unless `-no-expand-env` is given.

```yaml
addr: "127.0.0.1:9090"
dbs:
  - path: /data/app.db
    replica:
      url: s3://my-bucket/app
      endpoint: https://fly.storage.tigris.dev
litestream-rs:
  if-replication-stops: hold-start
  max-unreplicated-age: 10m
```

- `url` is `s3://<bucket>/<path>` or `file:///<dir>`. For the top of a bucket write `s3://bucket/` with the slash; `s3://bucket` without it is refused.
- `endpoint` names the service for anything but AWS S3. Without it the endpoint is AWS S3 in `region` (`us-east-1` unless set).
- `snapshot-read-limit` (in `litestream-rs`, off by default; a byte rate a second such as `30MB`, the sizes `part-size` takes) paces every read of the whole database file: the first copy of a database to an empty replica, a level-9 snapshot, and the scheduled verify's read of the live database. It is for an application that shares its page cache and disk with the replicator and slows down while a copy of a large database runs. At a limit the copy takes about twice the database's size over the limit (it reads every page twice), and it holds its read transaction for that long, so the WAL grows by the application's write rate over that time and the replica gets nothing newer than the copy's start until it ends. On an 8 GiB database with 10 commits a second, a limit of 1/16 of the disk's copy rate cut the application's read p99 by 86% and made the copy 9 times longer (WAL growth 32 MiB instead of 2 MiB); 1/4 cut it by 60% for a copy 2.7 times longer ([decisions/D309.md](decisions/D309.md)). Start at about a quarter of the disk's sequential read speed (`dd if=app.db of=/dev/null bs=1M` on a cold cache), then lower it while the application's latency during a copy is still too high. Reads made while the replicator holds the application's writes (the first start's check of the database against the replica) are never paced. The limit does not help when the application's hot pages nearly fill free memory: pacing lowers how fast the copy evicts them, not how much it reads. Go has no such setting.
- Settings Go does not have live under `litestream-rs`. Go warns of that section once at each start ("configuration contains unrecognized or duplicate key") and ignores it, so one file starts either program.
- `part-size` on the replica sets the multipart part size (`64MB`, for example): 5 MiB by default, 16 MiB on Tigris. One object holds at most 10,000 parts, so raise it when the database's compressed copy could pass 10,000 parts. The upload of a staged file holds no part: each part is read from the staging directory again as it is sent, checked against the first read, so that upload holds under a megabyte for each request in flight whatever the part size (D222). A compaction or a level-9 snapshot is written with no file behind it and holds the parts it has in flight plus the one it is filling, `part-size x (concurrency + 1)` as Go's uploader does (30 MiB at the defaults; about 1.1 GB at 100 MB parts with `concurrency: 10`), per database doing so at the same time. The setting is honoured whatever its size: nothing caps the memory but the two numbers, so lower either when that is too much, and `lsr` logs the product once for each S3 replica at start (D309). `concurrency` sets how many parts are on the wire at once for either kind; they are sent on up to `concurrency` threads of the store's own while the replicator goes on reading the next part (D233).
- The file is read strictly: an unknown key, a key set twice, or a feature that is not built is refused at load with the key and line. Run `lsr config check <file>` before a deploy.

[config-keys.md](config-keys.md) lists every key and what it sets.

## Credentials

`lsr` looks for S3 keys in this order, as Go's AWS SDK does:

1. `access-key-id` and `secret-access-key` in the config.
2. `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, with `AWS_SESSION_TOKEN` if set. `LITESTREAM_ACCESS_KEY_ID` and `LITESTREAM_SECRET_ACCESS_KEY` stand in when the `AWS_` names are unset.
3. A web identity token (`AWS_WEB_IDENTITY_TOKEN_FILE` with `AWS_ROLE_ARN`).
4. The profile `AWS_PROFILE` names in the shared files (`~/.aws/credentials`, `~/.aws/config`).
5. Container credentials, then the EC2 instance metadata service.

Temporary credentials are fetched again before they expire. A profile that needs an assumed role, IAM Identity Center or a login session is refused by name: those are not built ([#176](https://github.com/emschwartz/litestream-rs/issues/176)).

On AWS the keys need the policy in Litestream's S3 guide. Two more actions are optional: `s3:ListBucketMultipartUploads` on the bucket and `s3:AbortMultipartUpload` on its objects. With them `lsr` lists and aborts unfinished multipart uploads at every start, which Go never does. Without them that clean-up is skipped with one warning and nothing else fails ([s3-permissions.md](s3-permissions.md)).

## Running an application under -exec

```sh
lsr replicate -config litestream.yml -exec "myapp --port 8080"
```

`lsr` starts the command once every database is ready, forwards signals to it, and exits when it exits, after a final sync. Under the default `if-replication-stops: hold-start` the command is not started when a replicator stops at start or the store refuses its credentials. Add `-restore-if-db-not-exists` to restore a missing database from the replica first; with no backup yet it starts normally.

`-exec` is not required: other processes can write to the database while `lsr` runs beside them. For them, "held" means a write fails with SQLite's "database is locked" ([how-it-works.md](how-it-works.md)).

## Restore

```sh
lsr restore -o restored.db s3://my-bucket/app
lsr restore -config litestream.yml /data/app.db
lsr restore --replica s3://my-bucket/app --endpoint https://fly.storage.tigris.dev --output restored.db
```

The first form is Go's and reads a bare URL as AWS S3. The second restores the replica the config names for that database. The third takes the endpoint on the command line; `--endpoint` has no effect on a bare URL, so use `--replica` or a config for anything but AWS S3.

- A restore never overwrites a database. An existing output is an error unless `-force` is given, and then the old file stays until the new one is complete and checked. `-if-db-not-exists` exits 0 instead when the output exists.
- `-if-replica-exists` exits 0 without writing when the replica holds no files. A replica that has files and cannot meet the request is still an error.
- `-txid <16 hex digits>` restores the state right after that transaction. `-timestamp <RFC 3339>` restores the newest state created before that instant. An instant inside a merged file is refused unless `--allow-earlier` is given.
- `--integrity-check quick` or `full` runs SQLite's check on the result before it is put in place.
- Each restore records, in the restored database's working folder, that the database came from the replica, so a replicator started on it afterwards does not stop with `other_writer`.

`lsr verify --replica <url or dir>` reads and checks every file and reports what is restorable. `lsr files --replica <url or dir>` lists them.

## Log lines

Logs go to standard error in Go's `slog` text format, `time=... level=... msg=...`. The lines worth knowing:

| Line | Meaning |
|---|---|
| `writes blocked` with `held_by=booting` | Normal at start: writes are held until the start is judged |
| `comparing the database with the replica before starting`, then `writes blocked` with `held_by=checking_identity` | The working folder is missing, so the database is compared with the replica page by page. Writes wait for it; on a large database that takes a while |
| `writes allowed` | The hold is over |
| `replica position recovered` | Continuing from the WAL, with no snapshot |
| `replica position recovered; snapshot required` with `cause=` | A snapshot follows. `FirstSync` is a new replica; other causes say what broke continuity |
| `retrying` with `error=` and `retry_at=` | A fault that is being retried, for example `StoreTransient` while the store is unreachable |
| `replication degraded` with `task=` and `cause=` | A failure that persists. That task waits and retries |
| `writes blocked` with `held_by=refused_at_start` | The store refused a request the start needs (credentials, or a permission such as listing the bucket). Writes stay held until it accepts |
| `replicate refused: the replica holds transactions up to ...`, then `replication stopped` with `cause=OtherWriter` | The bucket holds files this database cannot account for. See "When it stops" |
| `database drained` with `txid=` | At shutdown, everything committed reached the replica |
| `database not drained: deadline reached, the replica lacks every commit after last_replicated_txid` | Shutdown ran out of time (`shutdown-sync-timeout`, 30 seconds by default) with changes not uploaded. A restore lacks every commit after `last_replicated_txid` (`none`: no transaction of this database was confirmed on the replica) until the next start uploads them; `unsynced_wal_bytes` and `staged_not_uploaded` say how much, counted high |
| `litestream shut down` with `clean=true` or `false` | The last line |

## Exit codes

Go's commands (`replicate`, `restore`, `databases`, `ltx`, `wal`) exit as Go does, so a script written for Go sees the same codes:

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Any failure. Under `-exec` that includes a child that failed, whatever its own code; the child's code is in the log |
| 4 | `replicate -exec` only: replication stopped, so the application was stopped or never started |

`lsr`'s own commands (`verify`, `files`, `trust-local`, `config check`) use a code per kind of failure:

| Code | Meaning |
|---|---|
| 0 | Success. For `verify`: nothing worse than a note |
| 1 | `verify` found a degraded replica |
| 2 | `verify` found the replica unrestorable, or a config was refused |
| 3 | A store or I/O failure |
| 64 | Usage error |

## Health and metrics

Set `addr` in the config and `lsr replicate` serves two paths on exactly that address:

- `/healthz`: JSON with `status`, the list `down`, and for each database its `state`, `stop_cause`, `degraded` tasks with their causes, `synced_txid`, `replicated_txid`, `unreplicated_since` and `held_by`. It answers 200 while every database is booting, running, or degraded and retrying. It answers 503 once a database has stopped, failed to start, or is holding the application.
- `/metrics`: Prometheus text. Go's `litestream_*` series that the engine can count, and `lsr_*` series, among them `lsr_unreplicated_age_seconds`, `lsr_replicated_txid`, `lsr_db_state` and `lsr_snapshots_total` by cause.

Alert on `lsr_unreplicated_age_seconds`, not on `/healthz` alone: a store that is unreachable keeps `/healthz` at 200 while `lsr` retries. The gauge climbs for the length of each whole-database copy ([limitations.md](limitations.md)), so set the threshold above that. Do not wire `/healthz` to a supervisor that restarts on 503: a restart forgets the degraded state and repeats the start.

## When it stops

A replicator that finds files in the bucket this database cannot account for stops with `other_writer` and, under `hold-start`, holds the application. The cause is one of three: another replicator writing to the same path, an empty or wrong database on this host, or a disk put back from an older copy (told apart by the replicator's counter, with one short exception in [limitations](limitations.md)). Choose one:

```sh
lsr trust-local /data/app.db --replica s3://my-bucket/app
lsr restore -force -o /data/app.db s3://my-bucket/app
```

The first says this database is the right one; the next start takes a snapshot above the bucket's files. The second says the bucket is right and replaces the database with it. Then start `lsr replicate` again.

## Moving from Go Litestream, and back

Never let Go and `lsr` run on one bucket path at the same moment, not even for the seconds of a rolling deploy: restores can then come back wrong with no error ([#346](https://github.com/emschwartz/litestream-rs/issues/346)). Nothing enforces it, because the working folder's lock does not exclude Go, and an application that embeds the library while a Go sidecar still runs is the same overlap. To swap them: stop the one that is running, confirm its process has exited, then start the other.

With Go stopped, `lsr` reads the same config and the same working folder (`.<name>-litestream` beside the database) and continues from Go's files, with no snapshot when the WAL is intact. Before the swap, run `lsr config check` (it refuses settings for features `lsr` lacks), add the two optional permissions above if the keys can have them, and keep free disk about the size of the database beside it: a start that cannot continue from the WAL stages a compressed copy there first. To go back, stop `lsr`, confirm it has exited, and start Go v0.5.16 or later.

### When the application runs under Go's -exec

Go stops its `-exec` child on shutdown, so the application's last connection closes. SQLite then checkpoints and deletes the WAL. `lsr`'s first start finds no WAL to continue from, so it stages a whole-database copy and takes a first snapshot. Two costs follow:

- Staging needs about twice the database's size in free disk beside it.
- New commits do not reach the replica until the copy is uploaded. On a real provider a 24 GiB database took about 4 minutes; scale by size. The application keeps running and its writes are kept in the WAL meanwhile.

To avoid the copy, stop the application first, outside Go, and only then stop Go. The WAL survives, and `lsr` continues from Go's position with no copy and no snapshot. This means the application cannot be Go's `-exec` child at that moment, so it is a choice made before the swap.

`lsr`'s own restarts do not repeat the cost: it keeps the WAL when its connection is the last to close, so the next start continues from it. The choice to accept the gap is recorded in [decisions/D291.md](decisions/D291.md).
