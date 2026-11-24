# How litestream-rs differs from Go Litestream

Compared with Go Litestream 0.5.x at upstream 041a8c3. This page describes v1 as decided in [decisions/D173.md](decisions/D173.md).
"S3" here means any S3-compatible object storage: AWS S3, Cloudflare R2, Tigris, MinIO and the like.

The replica is the same: the bucket layout and the LTX files. Either implementation restores what the other wrote (Go from v0.5.16 for files written here), and either takes over the other's bucket and local files with a plain restart.

## Same as Go

| Situation | What both do |
|---|---|
| The replicator crashes, is killed or restarts | Resume from the local files and the WAL. Take a snapshot only when the WAL no longer matches |
| The replicator restarts with files it had saved locally and not yet uploaded | Upload them as they are. litestream-rs checks every one first: its checksum, that it continues from the file before it, and that it matches the WAL where the WAL still holds those changes. Go checks only the newest. Three unlikely cases get past both, listed under [Files waiting to upload at a restart](#files-waiting-to-upload-at-a-restart) |
| S3 errors, throttles or is down | Retry, waiting up to 5 minutes between tries, for as long as it takes |
| SQLite changes the WAL: a checkpoint from outside, a WAL restart, `VACUUM`, a deleted WAL | Continue, or take a snapshot |
| S3's list of files is out of date | Trust the list. Object storage that lists behind its writes is unsupported |
| The replicator restarts while replication is failing | Not covered: the failure is forgotten. The count of failed tries and the degraded report start again from zero, so a replicator that keeps restarting can stay quiet. litestream-rs carries one number across the restart, the age of the oldest change that was staged and not yet in S3 (`lsr_unreplicated_age_seconds`) |
| A replica at the top of a bucket, with no folder inside it: `s3://bucket/` with the slash at the end, or `type: s3` with a `bucket` and no `path` | Works. Every object's name in the bucket starts with a slash, for example `/0000/<file>.ltx` ([decisions/D181.md](decisions/D181.md)) |
| `replicate` and `restore` fail | Exit 1 |
| A database with a long history is written to | Not the same cost. litestream-rs keeps the newest file of each level in memory and each pass lists only what its decision reads (nothing, when nothing is due), with a whole listing every 15 minutes and after any fault ([decisions/D193.md](decisions/D193.md)). About 680 requests an hour at 20,000 level-1 files. A file another writer puts at a compaction level below the newest known, a foreign snapshot, or a file above the configured top is noticed at the next whole listing, at most 15 minutes later |
| `l0-retention-check-interval` is not set | Not the same default. Go checks level 0 every 15 s; litestream-rs checks every 120 s, because each check lists level 0, a level-1 tail and level 9 fresh (about 3 requests, 5 with paging), which at 15 s is about 700 to 1,200 requests an hour on top of the 679 the whole hour costs at 120 s (D193). A level-0 file lives up to 2 minutes longer. A set value is honoured exactly, as in Go |

### Files waiting to upload at a restart

The replicator saves each batch of changes in its working folder (the folder where it keeps its own files) before it uploads it. After a restart it uploads whatever is still waiting there. Neither implementation catches these three cases ([decisions/D175.md](decisions/D175.md)):

- **The working folder is put back from a backup that does not match the database**, for example one taken before the database was rolled back or replaced. The files waiting in that folder are uploaded, although they belong to the other history. A restore of the newest state is still right. A restore to a point in time that those files cover returns the other history's data.
- **A waiting file is altered on purpose so that it still passes the checks.** It is uploaded.
- **The power fails on a disk that loses data it had reported as written.** The database can come back without its last changes while files that hold them are still waiting. Those files are uploaded, so the replica holds changes the database lost. If the application then writes different changes over the same part of the WAL, that is noticed only while the local copy of the uploaded file still exists, about 30 seconds after its upload. After that it is not noticed.

## Different: litestream-rs stops or refuses, Go carries on

| Situation | Go | litestream-rs | Why |
|---|---|---|---|
| At start, the bucket holds files this database cannot account for: another writer, a wrong or empty database, a disk put back from an older copy | Adopts the bucket's newest file and snapshots the local database on top | Stops with `other_writer` until an operator chooses. If the local database is the right one, run `lsr trust-local <db> --replica <url>` and start again. If the bucket is the right one, restore from it with `lsr restore` and start from the backup. Under `-exec` the application is not started. One exception: with a brand-new working folder, a database written after the bucket's newest file that holds at least half of its pages is taken to be the same database with changes that never reached the bucket, and carries on with a snapshot on top and a warning (`ahead_of_replica`, [decisions/D194.md](decisions/D194.md)) | A wrong or empty database would become the newest state, and retention would later delete the real history |
| Two instances write to one bucket path at the same time | Both write and their histories interleave. The docs call avoiding this the user's job | When both are litestream-rs on a store with conditional writes, uploads are create-only: the bucket refuses the second writer's file and that writer stops. Go overwrites without a condition, so litestream-rs cannot refuse Go's uploads. With Go writing at the same time, one transaction can end with two contents, and a restore can return the wrong one without an error. litestream-rs stops if it sees Go's file past its own position, but not when Go overwrites one it already wrote. Take turns with a restart; never run both at once. On a store without conditional writes litestream-rs writes as Go does and says so ([decisions/D079.md](decisions/D079.md)) | The backup can become impossible to restore |
| A file in the bucket is corrupt | `restore` does not check by default and can exit 0 with a corrupt database | Every file is verified before it is used. The restore fails with a named error | A restore must never return wrong pages |
| A restore the replica cannot give exactly, because files are missing | Can restore a shorter history. With `-if-replica-exists` it exits 0 and writes nothing | Fails with `Gap`. `-if-replica-exists` skips only a replica that holds nothing | A start script would otherwise run the application on an empty database beside its own history |
| A config that cannot mean what it says: an unknown key, a key set twice, a value no request could use | Starts, and ignores or misreads it | Refuses at load, naming the key and line | A typo in `retention` silently changes what is kept |
| Two databases share one working folder or one bucket path | Starts | Refuses at load | Each overwrites the other's files |
| A replica written `s3://bucket`, with no slash after the bucket name | Stores the replica in a folder literally named `.`, so every object's name starts with `./`. Whether Go means to is asked in [benbjohnson/litestream#1651](https://github.com/benbjohnson/litestream/issues/1651) | Refuses at load, with an error that says what to write. For the top of the bucket, write `s3://bucket/`. A replica Go already wrote under `./` cannot be read here: move its objects or start a new replica | The URL library litestream-rs sends its requests through removes a `.` from a path, as a browser does, so a request for an object under `./` would reach a different object ([decisions/D181.md](decisions/D181.md), [decisions/D186.md](decisions/D186.md)) |
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
| The working folder is lost and the database is kept | Adopts the bucket's newest file and snapshots on top | Compares the database with the bucket and continues when they are equal. When the database was written after the bucket's newest file and holds at least half of its pages, it takes a snapshot on top and warns (`ahead_of_replica`): the lost folder held changes the bucket never got. Otherwise it stops, as in the first row above ([decisions/D194.md](decisions/D194.md)) |
| The S3 provider has no conditional writes | Does not use them | Runs as Go does, and reports `conditional_writes_unsupported` |
| `restore` over an existing database | Deletes the old file first | Keeps the old file until the new one is complete and checked |
| `restore` by URL to a time after the newest file | Refuses | Restores the newest state |
| `replicate -once -enforce-retention` on an idle database | Keeps 1 snapshot | Keeps 2 |
| A TRUNCATE checkpoint, once the WAL passes `truncate-page-n` | On by default. After it Go copies the whole database while it holds SQLite's write lock, so the application cannot write until the copy ends (minutes, for a large database) | Off unless the config says `litestream-rs: {app-readers: none}`. Then each TRUNCATE that empties the WAL is followed by a copy of the whole database, read from disk and uploaded as a snapshot: disk reads and bucket bytes in proportion to the database's size. The application keeps writing during the copy. Writes wait only while SQLite's own TRUNCATE copies the WAL into the database file, which depends on the WAL's size, not the database's ([decisions/D190.md](decisions/D190.md)) |

## Go has it, litestream-rs does not

- Storage other than S3 and a local directory.
- The VFS read replica (Go's VFS reads a replica written here).
- The control socket and the commands that use it (`status`, `sync`, `list`, `info`), `reset`, `ltx`, `wal`, and the Windows service commands (issue #177).
- `/debug/pprof`, and the `/metrics` series the engine cannot count.
- The v0.3 `generations/` layout.

## litestream-rs has it, Go does not

- Holding the application back when replication cannot continue ([decisions/D177.md](decisions/D177.md)). Under Go the application always runs. Here the `if-replication-stops` setting decides, with three values:
  - `report`: the application is never held back. Health and metrics report the problem. This is what Go does.
  - `hold-start`, the default: the application is not started if the replicator stops at startup or S3 refuses it there, for example over wrong credentials. Once the application is running, a problem is only reported.
  - `stop-app`: as `hold-start` at startup. A running application is also held back when replication stops, or when changes have waited longer than `max-unreplicated-age` (10 minutes by default) to reach S3.

  What "held back" means: litestream-rs holds the database's write lock, so every write, from any process, fails with SQLite's "database is locked" (and the log says `writes blocked`). Under `-exec` the application is also not started, or is stopped. Writes are held the same way at startup until the replicator has checked the replica, except under `report` ([decisions/D192.md](decisions/D192.md)).