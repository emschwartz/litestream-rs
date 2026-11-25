# How litestream-rs differs from Go Litestream

Compared with Go Litestream 0.5.x at upstream 041a8c3. This page describes v1 as decided in [decisions/D173.md](decisions/D173.md).
"S3" here means any S3-compatible object storage: AWS S3, Cloudflare R2, Tigris, MinIO and the like.

The replica is the same: the bucket layout and the LTX files. Either implementation restores what the other wrote (Go from v0.5.16 for files written here), and either takes over the other's bucket and local files with a restart, once the other has fully stopped.

## The differences that matter most

- **Never run both at once on one bucket path.** Taking turns with a restart is safe. Go and litestream-rs writing together can leave restores that are wrong with no error ([#346](https://github.com/emschwartz/litestream-rs/issues/346)).
- **litestream-rs stops at start when the bucket holds files this database cannot account for** (`other_writer`). Go adopts the bucket and carries on. An operator chooses with `lsr trust-local` or `lsr restore`.
- **It can hold the application's writes.** Under the default `if-replication-stops: hold-start`, writes fail with "database is locked" until the start is judged, and stay held if that first attempt stops or is refused. `report` is Go's behaviour.
- **Restores are verified, and exact or refused.** A corrupt file or a missing one fails the restore with a named error, where Go can exit 0.
- **The config is read strictly.** An unknown key or a feature that is not built is refused at load.
- **No TRUNCATE checkpoint unless the config says the application holds no long readers.** Go truncates by default.
- **Missing:** storage other than S3 and a local directory, the VFS read replica, the control socket and its commands, and encryption settings.

## Same as Go

| Situation | What both do |
|---|---|
| The replicator crashes, is killed or restarts | Resume from the local files and the WAL. Take a snapshot only when the WAL no longer matches |
| The replicator restarts with files it had saved locally and not yet uploaded | Upload them as they are. litestream-rs checks every one first: its checksum, that it continues from the file before it, and that it matches the WAL where the WAL still holds those changes. Go checks only the newest. Three unlikely cases get past both, listed under [Files waiting to upload at a restart](#files-waiting-to-upload-at-a-restart) |
| S3 errors, throttles or is down | Retry, waiting up to 5 minutes between tries, for as long as it takes. Not the same in a long outage: litestream-rs stops staging once 4,096 files wait to upload, and from there the WAL holds the changes and grows. After a restart during the outage it stages and checkpoints nothing until S3 answers, where Go's local copy and checkpoints carry on |
| SQLite changes the WAL: a checkpoint from outside, a WAL restart, `VACUUM`, a deleted WAL | Continue, or take a snapshot |
| S3's list of files is out of date | Trust the list. Object storage that lists behind its writes is unsupported |
| The replicator restarts while replication is failing | Not covered: the failure is forgotten. The count of failed tries and the degraded report start again from zero, so a replicator that keeps restarting can stay quiet. litestream-rs carries one number across the restart, the age of the oldest change that was staged and not yet in S3 (`lsr_unreplicated_age_seconds`) |
| A replica at the top of a bucket, with no folder inside it: `s3://bucket/` with the slash at the end, or `type: s3` with a `bucket` and no `path` | Works. Every object's name in the bucket starts with a slash, for example `/0000/<file>.ltx` ([decisions/D181.md](decisions/D181.md)) |
| The S3 permissions Litestream's guide publishes | Work. litestream-rs also tries to list and abort open multipart uploads at start, and skips that sweep, with a warning, when the policy refuses it ([s3-permissions.md](s3-permissions.md)) |
| The store is down at start, with `hold-start` (litestream-rs only; Go never holds writes) | Starts the application at once | Holds writes while boot attempts fail, up to 15 seconds from the first failure, then releases them and starts the `-exec` child. One failed attempt does not release, because the next may find another writer ([decisions/D207.md](decisions/D207.md)) |
| `replicate` and `restore` fail | Exit 1 |

### Files waiting to upload at a restart

The replicator saves each batch of changes in its working folder (the folder where it keeps its own files) before it uploads it. After a restart it uploads whatever is still waiting there. Neither implementation catches these three cases ([decisions/D175.md](decisions/D175.md)):

- **The working folder is put back from a backup that does not match the database**, for example one taken before the database was rolled back or replaced. The files waiting in that folder are uploaded, although they belong to the other history. A restore of the newest state is still right. A restore to a point in time that those files cover returns the other history's data.
- **A waiting file is altered on purpose so that it still passes the checks.** It is uploaded.
- **The power fails on a disk that loses data it had reported as written.** The database can come back without its last changes while files that hold them are still waiting. Those files are uploaded, so the replica holds changes the database lost. If the application then writes different changes over the same part of the WAL, that is noticed only while the local copy of the uploaded file still exists, about 30 seconds after its upload. After that it is not noticed.

## Different: litestream-rs stops or refuses, Go carries on

| Situation | Go | litestream-rs | Why |
|---|---|---|---|
| At start, the bucket holds files this database cannot account for: another writer, a wrong or empty database, a disk put back from an older copy | Adopts the bucket's newest file and snapshots the local database on top | Stops with `other_writer` until an operator chooses. If the local database is the right one, run `lsr trust-local <db> --replica <url>` and start again. If the bucket is the right one, restore from it with `lsr restore` and start from the backup. Under `-exec` the application is not started. One exception: with a brand-new working folder, a database written after the bucket's newest file that holds at least half of its pages and has a replicator counter (`_litestream_seq`) at least as high as the bucket's is taken to be the same database with changes that never reached the bucket, and carries on with a snapshot on top and a warning (`ahead_of_replica`, [decisions/D194.md](decisions/D194.md), [decisions/D214.md](decisions/D214.md)). An older copy, whose counter is lower, stops; a copy taken after the bucket's last counter bump still passes, since the counter moves only at a checkpoint | A wrong or empty database would become the newest state, and retention would later delete the real history |
| Two instances write to one bucket path at the same time | Both write and their histories interleave. The docs call avoiding this the user's job | When both are litestream-rs on a store with conditional writes, uploads are create-only: the bucket refuses the second writer's file and that writer stops. Go overwrites without a condition, so litestream-rs cannot refuse Go's uploads. With Go writing at the same time, one transaction can end with two contents, and a restore can return the wrong one without an error. litestream-rs stops if it sees Go's file past its own position, but not when Go overwrites one it already wrote. Take turns with a restart; never run both at once. On a store without conditional writes litestream-rs writes as Go does and says so ([decisions/D079.md](decisions/D079.md)) | The backup can become impossible to restore |
| A file in the bucket is corrupt | `restore` does not check by default and can exit 0 with a corrupt database | Every file is verified before it is used. The restore fails with a named error | A restore must never return wrong pages |
| A restore the replica cannot give exactly, because files are missing | Can restore a shorter history. With `-if-replica-exists` it exits 0 and writes nothing | Fails with `Gap`. `-if-replica-exists` skips only a replica that holds nothing | A start script would otherwise run the application on an empty database beside its own history |
| A config that cannot mean what it says: an unknown key, a key set twice, a value no request could use | Starts, and ignores or misreads it | Refuses at load, naming the key and line | A typo in `retention` silently changes what is kept |
| Two databases share one working folder or one bucket path | Starts | Refuses at load | Each overwrites the other's files |
| A replica written `s3://bucket`, with no slash after the bucket name | Stores the replica in a folder literally named `.`, so every object's name starts with `./`. Whether Go means to is asked in [benbjohnson/litestream#1651](https://github.com/benbjohnson/litestream/issues/1651) | Refuses at load, with an error that says what to write. For the top of the bucket, write `s3://bucket/`. A replica Go already wrote under `./` cannot be read here: move its objects or start a new replica | The URL library litestream-rs sends its requests through removes a `.` from a path, as a browser does, so a request for an object under `./` would reach a different object ([decisions/D181.md](decisions/D181.md), [decisions/D186.md](decisions/D186.md)) |
| Used as a library: the application runs SQLite at `synchronous = OFF`, or links a SQLite build outside the verified list (SQLite 3.51.3 to 3.54.1 as `libsqlite3-sys` 0.37 to 0.40 bundle it) | Does not apply | Refuses the connection, or refuses to start | OFF syncs nothing; a build nobody verified may lay out the files the engine reads differently |
| A listing page repeats a key, a later page lists a file at or before one already passed, or the continuation token comes back | Takes the pages as they come (it does not look at order) | Refuses the listing as a retryable fault, so a cursor that went back cannot hide a file or count one twice. Keys within a page may come in any order (Cloudflare R2 lists a name after the longer names that extend it) and are accepted |

## Different: litestream-rs carries on as Go does, and checks or says more

| Situation | Go | litestream-rs |
|---|---|---|
| Power loss | Never syncs the WAL, so the bucket can hold commits the database then loses | Syncs the WAL before copying from it. If the disk loses them anyway, the next start takes a snapshot |
| A write of the WAL to disk fails | Does not notice | Reports `wal_sync_failed` and retries |
| Files are deleted from the bucket, or it is rolled back | Does not notice. Restores fail until the next snapshot | The same, and reports `remote_regressed` |
| The WAL is damaged below the last commit | Reads it as the end of the WAL | Reports `wal_corrupt` and replicates nothing past the damage |
| A writer stopped mid-commit left frames in the WAL | Releases through v0.5.18 can replicate them | Never reads past the last commit |
| A failure that persists | A log line | A named degraded state in `health()`, `/healthz` and `/metrics` |
| A store that refuses the credentials or cannot be reached at start | `litestream_sync_error_count` rises on each failed start of the database (and only there: an upload that fails later is only logged) | The same series rises the same way, once per failed start attempt, and `litestream_db_size`, `_txid` and `_wal_size` read 0 until the first sync. The `lsr_*` series report the rest |
| Log lines a deploy script may grep | `snapshot complete` and `compaction complete` at info level (`system=store db=<name>`, then `txid` or `level`, `txid.min`, `txid.max`, `size`) | The same messages, level and attributes, one line per snapshot and per compaction. Go's reasons for a snapshot (`wal truncated by another process`, `wal header salt reset, snapshotting`, and the others) are not messages: Go writes them as a `reason` of its debug `sync` line, and this daemon says `replica position recovered; snapshot required` (at boot) or `snapshot required` with a `cause` at info level instead. A script that greps Go's reasons finds nothing here |
| `litestream version` | Prints the version on one line; `-verbose` adds the Go version, commit, commit time and OS/arch | `lsr version` prints the version on one line; `-verbose` prints `Version` and `OS/Arch` only (this is not a Go build) |
| Two replicators start on one working folder (an overlapping deploy on one disk) | Runs both, which can corrupt the local files | The second `lsr replicate` waits for the first to exit. Used as a library, the second start is refused |
| The working folder is lost and the database is kept | Adopts the bucket's newest file and snapshots on top | Compares the database with the bucket and continues when they are equal. When the database was written after the bucket's newest file, holds at least half of its pages and does not carry a replicator counter below the bucket's, it takes a snapshot on top and warns (`ahead_of_replica`): the lost folder held changes the bucket never got. Otherwise it stops, as in the first row above, which includes a database that is an older copy of the bucket's ([decisions/D194.md](decisions/D194.md), [decisions/D214.md](decisions/D214.md)) |
| The S3 provider has no conditional writes | Does not use them | Runs as Go does, and reports `conditional_writes_unsupported` |
| `restore` over an existing database | Deletes the old file first | Keeps the old file until the new one is complete and checked. One open exception: after a restore that was killed at its last step, `-force` rewrites the database in place |
| `restore` by URL to a time after the newest file | Refuses | Restores the newest state |
| `restore` leaves a record for the replicator that starts next | Leaves only the database | Leaves only the database too, unless a config names the restored path (the one `-config`, `$LITESTREAM_CONFIG` or `/etc/litestream.yml` names): then it writes the takeover record in that database's working folder (its `meta-path`, else `.<name>-litestream` beside it) and prints `takeover recorded through <txid>`, so the replicator started on it builds on the bucket it came from. A backup or a copy no config names gets no folder. If such a file is replicated later and the working folder is new, the start compares it with the bucket and stops with `other_writer` when it cannot vouch for it; `lsr trust-local` is the one command that clears it ([decisions/D215.md](decisions/D215.md)) |
| A config shared with Go carries a `litestream-rs:` section | Warns once at each start (`configuration contains unrecognized or duplicate key`, `line N: field litestream-rs not found in type main.Config`) and ignores it, so the same file starts either program | Reads it |
| An environment variable in the config that is not set | Expands to nothing and says nothing, wherever it stands in the file, a YAML comment included | Expands to nothing as Go does. Warns once per name when a key or value reads it; a reference only in a comment is silent |
| `replicate -once -enforce-retention` on an idle database | Keeps 1 snapshot | Keeps 2 |
| A TRUNCATE checkpoint, once the WAL passes `truncate-page-n` | On by default. After it Go copies the whole database while it holds SQLite's write lock, so the application cannot write until the copy ends (minutes, for a large database) | Off unless the config says `litestream-rs: {app-readers: none}`. Then each TRUNCATE that empties the WAL is followed by a copy of the whole database, read from disk and uploaded as a snapshot: disk reads and bucket bytes in proportion to the database's size. The application keeps writing during the copy. Writes wait only while SQLite's own TRUNCATE copies the WAL into the database file, which depends on the WAL's size, not the database's ([decisions/D190.md](decisions/D190.md)) |

## Go has it, litestream-rs does not

- Storage other than S3 and a local directory.
- The VFS read replica (Go's VFS reads a replica written here).
- The control socket and the commands that use it (`status`, `sync`, `list`, `info`), `reset`, `restore -f` (follow mode), and the Windows service commands ([#177](https://github.com/emschwartz/litestream-rs/issues/177)). `databases`, `ltx` and `wal` are built.
- Settings for features that are not built, each refused at load: server-side encryption, storage classes, age encryption, heartbeats, `watch` on a directory, `auto-recover` and the MCP server ([config-keys.md](config-keys.md)).
- S3 credentials from an assumed role, IAM Identity Center or a login session ([#176](https://github.com/emschwartz/litestream-rs/issues/176)).
- `/debug/pprof`, and the `/metrics` series the engine cannot count.
- The v0.3 `generations/` layout.

## litestream-rs has it, Go does not

- Holding the application back when replication cannot continue ([decisions/D177.md](decisions/D177.md)). Under Go the application always runs. Here the `if-replication-stops` setting decides, with three values:
  - `report`: the application is never held back. Health and metrics report the problem. This is what Go does.
  - `hold-start`, the default: the application is not started if the replicator stops at startup or S3 refuses it there, for example over wrong credentials. Once the application is running, a problem is only reported.
  - `stop-app`: as `hold-start` at startup. A running application is also held back when replication stops, or when changes have waited longer than `max-unreplicated-age` (10 minutes by default) to reach S3.

  What "held back" means: litestream-rs holds the database's write lock, so every write, from any process, fails with SQLite's "database is locked" (and the log says `writes blocked`). Under `-exec` the application is also not started, or is stopped. Writes are held the same way at startup until the replicator has checked the replica and decided how to start, except under `report` ([decisions/D192.md](decisions/D192.md)). Go never holds writes at startup. The hold does not cover the first snapshot, which is taken while the application writes; a problem only that snapshot finds is handled like any later one ([decisions/D201.md](decisions/D201.md)). A start with a brand-new working folder holds writes while it compares the database with the bucket, about as long as a restore (`held_by=checking_identity`).