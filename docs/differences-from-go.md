# How litestream-rs differs from Go Litestream

Compared with Go Litestream 0.5.x at upstream 041a8c3. This page describes v1 as decided in [decisions/D173.md](decisions/D173.md).
"S3" here means any S3-compatible object storage: AWS S3, Cloudflare R2, Tigris, MinIO and the like.

The replica is the same: the bucket layout and the LTX files. Either implementation restores what the other wrote (Go from v0.5.16 for files written here), and either takes over the other's bucket and local files with a plain restart.

## Same as Go

| Situation | What both do |
|---|---|
| The replicator crashes, is killed or restarts | Resume from the local files and the WAL. Take a snapshot only when the WAL no longer matches |
| The replicator restarts with staged files that are not yet in S3 | Upload them as they are. litestream-rs first verifies each file's checksum, checks that it follows on from the file before it, and compares the files the WAL still holds with the WAL (Go checks only the newest). Not covered by either: a working folder put back alone from a copy of another history (its files are uploaded; a restore of the newest state is right, and a restore to a point inside that stretch returns the other history), a file forged to pass the checks, and a disk that loses written data at a power loss when the same part of the WAL is rewritten after the uploaded file's local copy is gone, about 30 seconds after its upload ([decisions/D175.md](decisions/D175.md)) |
| S3 errors, throttles or is down | Retry, waiting up to 5 minutes between tries, for as long as it takes |
| SQLite changes the WAL: a checkpoint from outside, a WAL restart, `VACUUM`, a deleted WAL | Continue, or take a snapshot |
| S3's list of files is out of date | Trust the list. Object storage that lists behind its writes is unsupported |
| The replicator restarts while replication is failing | Not covered: the failure is forgotten. The count of failed tries and the degraded report start again from zero, so a replicator that keeps restarting can stay quiet. litestream-rs carries one number across the restart, the age of the oldest change that was staged and not yet in S3 (`lsr_unreplicated_age_seconds`) |
| An S3 replica with no path inside the bucket, written `s3://bucket/` or as `bucket` with no `path` | Works, with keys that start with `/` ([decisions/D181.md](decisions/D181.md)) |
| `replicate` and `restore` fail | Exit 1 |

## Different: litestream-rs stops or refuses, Go carries on

| Situation | Go | litestream-rs | Why |
|---|---|---|---|
| At start, the bucket holds files this database cannot account for: another writer, a wrong or empty database, a disk put back from an older copy | Adopts the bucket's newest file and snapshots the local database on top | Stops with `other_writer` until an operator runs `lsr take-over`. Under `-exec` the application is not started | A wrong or empty database would become the newest state, and retention would later delete the real history |
| Two instances write to one bucket path at the same time | Both write and their histories interleave. The docs call avoiding this the user's job | Uploads are create-only, so the bucket refuses the second writer's file and that writer stops | The backup can become impossible to restore |
| A file in the bucket is corrupt | `restore` does not check by default and can exit 0 with a corrupt database | Every file is verified before it is used. The restore fails with a named error | A restore must never return wrong pages |
| A restore the replica cannot give exactly, because files are missing | Can restore a shorter history. With `-if-replica-exists` it exits 0 and writes nothing | Fails with `Gap`. `-if-replica-exists` skips only a replica that holds nothing | A start script would otherwise run the application on an empty database beside its own history |
| A config that cannot mean what it says: an unknown key, a key set twice, a value no request could use | Starts, and ignores or misreads it | Refuses at load, naming the key and line | A typo in `retention` silently changes what is kept |
| Two databases share one working folder or one bucket path | Starts | Refuses at load | Each overwrites the other's files |
| An S3 replica written `s3://bucket`, with no slash after the bucket | Reads the path as `.` and writes keys that start with `./` | Refuses at load; `s3://bucket/` is the bucket's root | No HTTP client sends a `.` segment, so the request would name another object than the key says ([decisions/D181.md](decisions/D181.md)) |
| Used as a library: the application runs SQLite at `synchronous = OFF`, or links another SQLite build than the bundled one | Does not apply | Refuses the connection, or refuses to start | OFF syncs nothing; another build lays out the files the engine reads differently |

## Different: litestream-rs carries on as Go does, and checks or says more

| Situation | Go | litestream-rs |
|---|---|---|
| Power loss | Never syncs the WAL, so the bucket can hold commits the database then loses | Syncs the WAL before copying from it. If the disk loses them anyway, the next start takes a snapshot |
| A write of the WAL to disk fails | Does not notice | Reports `wal_sync_failed` and retries |
| Files are deleted from the bucket, or it is rolled back | Does not notice. Restores fail until the next snapshot | The same, and reports `remote_regressed` |
| The WAL is damaged below the last commit | Reads it as the end of the WAL | Reports `wal_corrupt` and replicates nothing past the damage |
| A writer stopped mid-commit left frames in the WAL | Releases through v0.5.18 can replicate them | Never reads past the last commit |
| A failure that persists | A log line | A named degraded state in `health()`, `/healthz` and `/metrics` |
| Two replicators start on one working folder (an overlapping deploy on one disk) | Runs both, which can corrupt the local files | The second waits for the first to exit |
| The working folder is lost and the database is kept | Adopts the bucket's newest file and snapshots on top | Compares the database with the bucket and continues when they are equal. Otherwise it stops, as in the first row above |
| The S3 provider has no conditional writes | Does not use them | Runs as Go does, and reports `conditional_writes_unsupported` |
| `restore` over an existing database | Deletes the old file first | Keeps the old file until the new one is complete and checked |
| `restore` by URL to a time after the newest file | Refuses | Restores the newest state |
| `replicate -once -enforce-retention` on an idle database | Keeps 1 snapshot | Keeps 2 |

## Go has it, litestream-rs does not

- Storage other than S3 and a local directory.
- The VFS read replica (Go's VFS reads a replica written here).
- The control socket and the commands that use it (`status`, `sync`, `list`, `info`), `reset`, `ltx`, `wal`, and the Windows service commands (issue #177).
- `/debug/pprof`, and the `/metrics` series the engine cannot count.
- The v0.3 `generations/` layout.

## litestream-rs has it, Go does not

- Holding the application when replication stops (`if-replication-stops`, [decisions/D177.md](decisions/D177.md)). The default, `hold-start`, does not start the application when the replicator stops or S3 refuses it at start. `report` never holds the application, as under Go. `stop-app` also holds a running application's writes when replication stops or changes have waited too long for S3.

