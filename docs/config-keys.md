# Config keys for library users

Generated from `crates/config/src/keys.rs` by `crates/config/src/keys/table.rs`; `LSR_CONFIG_KEYS_WRITE=1 cargo nextest run -p lsr-config -E 'test(=keys::table::the_key_table_is_fresh)'` rewrites it, and the test fails when it is stale.

`lsr_config::load_plan` reads a `litestream.yml` and returns a `Plan`: per database a `lsr_replica::Config`, a `ReplicaPlan` (open it with `lsr_config::open_store`), and the restore-if-missing setting. The tables say what each key sets in it. "Daemon only" keys are read by `lsr replicate` and are not in a plan.

`busy-timeout` has no effect, and the reason is worth a sentence: Go's `busy-timeout` is how long Go's own SQLite connections wait on a lock, while litestream-rs's connections never wait (a busy timeout of zero) and retry on their own backoff; the TRUNCATE wait it guarded in Go is replaced by the reader-guard rule, which runs TRUNCATE only under `litestream-rs.app-readers: none` (`ReaderTracking::Tracked`).

## Top level

| Key | What it does |
| --- | --- |
| `addr` | Daemon only: the address of `/metrics` and `/healthz`. No field in a plan. |
| `socket` | Not supported (the control socket). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `levels` | `Config.maintenance`: the compaction levels, level 1 first. |
| `snapshot` | A section; its keys are below. |
| `validation` | A section; its keys are below. |
| `l0-retention` | `Config.maintenance`: how long a covered level 0 file is kept before it is deleted; 0 keeps every one. |
| `l0-retention-check-interval` | `Config.maintenance`: how often the level 0 retention pass runs (whole milliseconds, above 0). Unset, it runs every 120 s (up to 240 s while a compaction job waits), not Go's 15 s (docs/decisions/D193.md); set, exactly at the value. |
| `verify-compaction` | Not supported (compaction verification). Accepted at `false` or empty; any other value is refused. |
| `retention` | A section; its keys are below. |
| `heartbeat-url` | Not supported (heartbeats). Accepted at empty; any other value is refused. |
| `heartbeat-interval` | Not supported (heartbeats). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `dbs` | One entry of `Plan::dbs` per database; a `dir` entry gives one for each database it finds. |
| `exec` | Daemon only: the command the daemon runs and supervises. No field in a plan. |
| `logging` | A section; its keys are below. |
| `mcp-addr` | Not supported (the MCP server). Accepted at empty; any other value is refused. |
| `shutdown-sync-timeout` | `Config.shutdown_grace` of every database, and `Plan::shutdown_sync_timeout`. |
| `shutdown-sync-interval` | No effect: litestream-rs's final sync retries on its own schedule. |
| `shutdown-timeout` | No effect: litestream-rs bounds shutdown by shutdown-sync-timeout alone. |
| `part-size` | No effect: Go does not copy a top-level part-size into replicas. |
| `litestream-rs` | A section; its keys are below. |

## Replica settings (at the top level as the default for every replica, or inside a replica)

| Key | What it does |
| --- | --- |
| `sync-interval` | `Config.retry_interval`: the first wait after a failed upload. |
| `validation-interval` | No effect: Go parses it and never reads it; the scheduled verify's interval is validation.interval. |
| `max-sync-ltx-files` | `Config.sync_limits.files_per_upload`; 0 or less means every staged file. |
| `auto-recover` | Not supported (auto-recover). Accepted at `false` or empty; any other value is refused. |
| `access-key-id` | The store: `Credentials::FromConfig`, with `secret-access-key`. Without both, the AWS environment variables and shared files. |
| `secret-access-key` | The store: see `access-key-id`. |
| `region` | The store: the `S3Config` region. |
| `bucket` | The store: the `S3Config` bucket. |
| `endpoint` | The store: the `S3Config` endpoint. |
| `force-path-style` | The store: path-style or virtual-host-style addressing. |
| `sign-payload` | No effect: litestream-rs's S3 client signs requests one fixed way. |
| `sign-accept-encoding` | No effect: litestream-rs's S3 client signs requests one fixed way. |
| `require-content-md5` | No effect: litestream-rs always sends Content-MD5 on uploads. |
| `put-object-content-md5` | No effect: litestream-rs always sends Content-MD5 on uploads. |
| `skip-verify` | No effect: litestream-rs always verifies TLS certificates. |
| `storage-class` | Not supported (an S3 storage class). Accepted at empty; any other value is refused. |
| `part-size` | The store: the `S3Config` multipart part size. |
| `concurrency` | The store: how many multipart parts are in flight. |
| `sse-customer-algorithm` | Not supported (server-side encryption). Accepted at empty; any other value is refused. |
| `sse-customer-key` | Not supported (server-side encryption). Accepted at empty; any other value is refused. |
| `sse-customer-key-path` | Not supported (server-side encryption). Accepted at empty; any other value is refused. |
| `sse-kms-key-id` | Not supported (server-side encryption). Accepted at empty; any other value is refused. |
| `account-name` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `account-key` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `sas-token` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `host` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `user` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `password` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `key-path` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `concurrent-writes` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `host-key` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `webdav-url` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `webdav-username` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `webdav-password` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `jwt` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `seed` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `creds` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `nkey` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `username` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `token` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `tls` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `root-cas` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `client-cert` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `client-key` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `max-reconnects` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `reconnect-wait` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `timeout` | Not supported (a replica backend other than s3 and file). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `age` | Not supported (age encryption). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |

## `snapshot` (top level, or inside a `dbs` entry)

| Key | What it does |
| --- | --- |
| `interval` | `Config.maintenance`: how often a snapshot is written. A value inside a `dbs` entry is promoted to the one setting; every entry that sets it must agree. |
| `retention` | `Config.maintenance`: how long snapshots are kept. A value inside a `dbs` entry is promoted, as for `interval`. |

## `retention`

| Key | What it does |
| --- | --- |
| `enabled` | `Config.maintenance`: `true` deletes old files from the replica, `false` keeps every file. |

## `socket`

| Key | What it does |
| --- | --- |
| `enabled` | Not supported (the control socket). Accepted at `false` or empty; any other value is refused. |
| `path` | Not supported (the control socket). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |
| `permissions` | Not supported (the control socket). Accepted at any value, since it matters only with a feature that is refused; any other value is refused. |

## `validation`

| Key | What it does |
| --- | --- |
| `interval` | `Config.verify_interval`: how often the scheduled verify runs; 0 or less never. |

## `logging`

| Key | What it does |
| --- | --- |
| `level` | Daemon only; no field in a plan. |
| `type` | Daemon only; no field in a plan. |
| `stderr` | Daemon only; no field in a plan. |
| `source` | No effect: litestream-rs's log lines carry no source location. |

## `levels` entries

| Key | What it does |
| --- | --- |
| `interval` | `Config.maintenance`: one level's interval. |

## `dbs` entries

| Key | What it does |
| --- | --- |
| `path` | `DbPlan::path`: one database. |
| `dir` | `DbPlan::path` of each database found in the directory. |
| `pattern` | Which files in `dir` are databases. |
| `recursive` | Whether `dir` is searched in subdirectories. |
| `watch` | Not supported (watching a directory for new databases). Accepted at `false` or empty; any other value is refused. |
| `snapshot` | `Config.maintenance`: promoted to the top-level `snapshot`; see there. |
| `meta-path` | `Config.meta_dir` of a `path` entry; the default is `.<name>-litestream` beside the database. |
| `meta-dir` | `Config.meta_dir` of each database a `dir` entry finds. |
| `monitor-interval` | `Config.poll_interval`: how often the WAL is polled, and how often a missing database file is looked for by the daemon. |
| `checkpoint-interval` | `Config.checkpoint.interval`: how often a PASSIVE checkpoint runs on a quiet WAL; 0 never. |
| `busy-timeout` | No effect: litestream-rs's connections run with busy timeout zero. |
| `min-checkpoint-page-count` | `Config.checkpoint.min_pages`: WAL pages that trigger a PASSIVE checkpoint. |
| `truncate-page-n` | `Config.checkpoint.truncate_pages`: WAL pages that trigger a TRUNCATE checkpoint. Takes effect only with `litestream-rs.app-readers: none`; 0 or less never truncates. |
| `max-sync-wal-bytes` | `Config.sync_limits.wal_bytes_per_step`; 0 or less means no cap. |
| `restore-if-db-not-exists` | `DbPlan::restore_if_missing`: only on a `path` entry. The caller restores; see `restore_if_missing` in `lsr-replica`. |
| `replica` | `DbPlan::replica`: where the database replicates to. |
| `replicas` | `DbPlan::replica`: a list of exactly one replica. |

## `dbs[].replica` (or an entry of `replicas`)

| Key | What it does |
| --- | --- |
| `type` | `DbPlan::replica`: `s3` or `file`. |
| `name` | No effect: replica names were for v0.3's multiple replicas. |
| `path` | `DbPlan::replica`: the directory of a `file` replica, or the prefix of an `s3` one. |
| `url` | `DbPlan::replica`: the whole replica as a URL (`s3://` or `file://`). |

## `age`

| Key | What it does |
| --- | --- |
| `identities` | Not supported (age encryption). Accepted at an empty list; any other value is refused. |
| `recipients` | Not supported (age encryption). Accepted at an empty list; any other value is refused. |

## `litestream-rs` (read only by litestream-rs; Go ignores it)

| Key | What it does |
| --- | --- |
| `app-readers` | `Config.readers`: `none` is `ReaderTracking::Tracked`, which lets TRUNCATE run. Its cost: each TRUNCATE that empties the WAL is followed by a copy of the whole database, read from disk and uploaded as a snapshot. The application keeps writing during the copy; writes wait only while SQLite's TRUNCATE copies the WAL into the database file. |
| `app-autocheckpoint` | `Config.app_autocheckpoint`. |
| `responses` | A section; its keys are below. |
| `if-replication-stops` | `Config.if_replication_stops`. |
| `max-unreplicated-age` | `Config.max_unreplicated_age`. |

## `litestream-rs.responses`

| Key | What it does |
| --- | --- |
| `credentials` | `Config.responses.credentials`. |
| `permission-denied` | `Config.responses.permission_denied`. |
| `clock-skew` | `Config.responses.clock_skew`. |
| `bucket-missing` | `Config.responses.bucket_missing`. |
| `tls` | `Config.responses.tls`. |
| `store-refused` | `Config.responses.store_refused`. |
| `remote-corrupt` | `Config.responses.remote_corrupt`. |
| `remote-regressed` | `Config.responses.remote_regressed`. |
| `wal-corrupt` | `Config.responses.wal_corrupt`. |
| `wal-truncated` | `Config.responses.wal_truncated`. |
| `wal-unlinked` | `Config.responses.wal_unlinked`. |
| `wal-sync-failed` | `Config.responses.wal_sync_failed`. |
| `page-size-changed` | `Config.responses.page_size_changed`. |
| `conditional-writes-unsupported` | `Config.responses.conditional_writes_unsupported`. |
| `bug` | `Config.responses.bug`. |
| `other-writer` | `Config.responses.other_writer`. |
