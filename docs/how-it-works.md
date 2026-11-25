# How litestream-rs works

One page on the mechanism. [go-terms.md](go-terms.md) maps the words used here to Go Litestream's, LTX's and SQLite's. [BEHAVIOR.md](BEHAVIOR.md) is the case-by-case account.

```text
 application ──commits──▶ app.db-wal ◀──read lock── replicator (one thread)
                                                         │ every second: sync the WAL, read new commits
                                                         ▼
                              .app.db-litestream/ltx/0/   staged LTX files
                                                         │ upload, create-only
                                                         ▼
 bucket/prefix/  0000/ ──merge──▶ 0001/ ──▶ 0002/ ──▶ 0003/        0009/ snapshots
```

## The WAL and the read lock

SQLite in WAL mode appends each commit to `app.db-wal` and later copies those pages into the database file (a checkpoint). Once a checkpoint has copied everything, the next writer starts the WAL again from the top, and the old frames are gone.

The replicator holds a read transaction on the database from before the application's first write. That read lock stops any checkpoint from restarting the WAL past frames the replicator has not read. The replicator then runs the checkpoints itself, each one only after it has read through the WAL and staged what it found: a PASSIVE checkpoint once the WAL holds 1,000 pages, or once a minute. A TRUNCATE checkpoint runs only when the config says the application holds no long readers (`app-readers: none`).

It reads only up to the last commit SQLite's wal-index records, never to the end of the file, so frames left by a writer that stopped mid-commit are never replicated.

## LTX files and levels

Each batch of changes becomes one LTX file: a header, the changed pages (LZ4 compressed), a page index and a trailer with checksums. Its name is the range of transaction ids it covers, `<first>-<last>.ltx`, sixteen hex digits each. These are LTX transaction ids, which number the replicator's level-0 files in order from 1. They are not SQLite values.

Files live in levels, ten folders named `0000` to `0009` under the replica's path in a bucket (`ltx/0` to `ltx/9` in a directory replica):

| Level | Holds | Written |
|---|---|---|
| 0 | One file per sync: the commits since the last one | About once a second while the application writes |
| 1, 2, 3 | Merges of the level below | Every 30 seconds, 5 minutes and 1 hour by default |
| 4 to 8 | Further merge levels | Only when the config adds them |
| 9 | Snapshots: the whole database at one transaction | Every 24 hours by default, and when continuity is lost |

A merged file holds the newest copy of each page from its inputs, so a restore reads one hourly file instead of thousands of small ones.

## The sync loop

One thread per database does everything, one unit of work at a time, in this order of priority:

1. **Sync.** Every second it reads the wal-index. If commits arrived, it syncs the WAL to disk, reads the new commits, and writes them as one level-0 file in the working folder. Syncing first means the bucket never holds a commit that a power loss could take from the database on an honest disk.
2. **Checkpoint**, when one is due, in the same step as a sync that reached the WAL's end.
3. **Upload** each staged file, 8 MiB a turn: a larger file is sent over several turns, with syncs and checkpoints in between, and appears on the replica only when it is whole. Every file is verified on the read that uploads it and sent with a checksum. Uploads are create-only where the store supports it, so a second writer's file is refused instead of overwritten.
4. **Maintenance**: merges, snapshots and deletes, planned from a listing of the bucket. A restarted process reading the same listing plans the same job.

A failure in one task does not stop the others, but a long unit does: while the thread copies the whole database (a first sync, a snapshot), nothing new is synced or uploaded. Each retry waits longer, up to 5 minutes, and a failure that persists becomes a named degraded state.

## Compaction, snapshots and retention

A level is merged when its window has passed and the level below holds files above it. One job merges at most 32 input files, in one unbroken run of transaction ids. A merge never deletes its inputs, so a crash at any point leaves a replica that restores.

A snapshot is a level-9 file holding every page. One is taken on the schedule, and whenever the replicator cannot prove the WAL continues from what it last replicated: a first sync, a deleted or restarted WAL, a `VACUUM`, a database that went backwards. Each snapshot records its cause.

Deletes are a separate job. By default a level-0 file goes 5 minutes after a level-1 file covers it, and snapshots are kept for 24 hours. A file is deleted only when a higher level covers it and the remaining files still restore the newest state. While any task is degraded, the restore point the replica held when the trouble began is kept.

## Restore

A restore lists the replica and plans a chain: the newest snapshot at or below the target, then the files that continue from it without a hole, up to the target. The target is the newest state, a transaction id, or an instant.

It checks every file's checksum before the result is put in place. It builds the database in a scratch file beside the target, syncs it, and only then puts it in place, so the target is absent, as it was, or complete (one open exception with `-force` is in [limitations.md](limitations.md)). If the chain has a hole the restore fails with `Gap`. It never returns a shorter history.

Two limits follow from merging. A transaction older than the level-0 retention can be restored only where a remaining file ends on it. An instant that falls inside a merged file is refused unless `--allow-earlier` asks for the newest state before it.

## The check at start

At start the replicator decides how to continue, from the working folder, the WAL and a listing of the bucket:

- The staged files and the WAL agree with the bucket: continue where it left off, with no snapshot.
- The WAL no longer matches (it was deleted, restarted or rewound): take a snapshot, with the cause named.
- The bucket holds files above anything this database can account for: stop with `other_writer`.

That last case is the one place it stops by default. Another replicator writing to the same path, an empty or wrong database, and a disk put back from an older copy all look the same from here, and carrying on would make the wrong database the newest state. An operator chooses: `lsr trust-local` if the database is right, `lsr restore` if the bucket is.

A working folder that was lost is not a stop when the database can speak for itself. The replicator compares the database with the bucket's newest state page by page. If they are equal it continues. If the database was written after the bucket's newest file and still holds at least half of its pages, it takes a snapshot on top and warns (`ahead_of_replica`). An older copy of the database whose modified time is new, as after a plain `cp`, passes the same test, and the replica's newest state then goes backwards.

## What stops replication, and what blocking writes means

By default every failure that lasts is retried without end and reported as degraded: wrong credentials, a missing bucket, a store that is down, a full disk, a corrupt WAL. Only another writer stops it. The `litestream-rs.responses` config section can turn any of these into a stop.

Separately, `if-replication-stops` decides what a stopped or stuck replicator does to the application:

| Value | At start | While running |
|---|---|---|
| `report` | Writes are never held. This is what Go does | Report only |
| `hold-start` (default) | Writes are held until the start is judged, and stay held if that first attempt stops or the store refuses it (wrong credentials, a missing permission). Best effort: a first attempt that fails any other way releases them | Report only |
| `stop-app` | As `hold-start` | Writes are also held when replication stops, or when a commit has waited longer than `max-unreplicated-age` (10 minutes by default) and a fresh attempt fails |

"Held" means the replicator keeps SQLite's write lock, so every write, from any process, fails with "database is locked". Reads go on. Under `lsr replicate -exec` the application is also not started, or is stopped. A store that is down does not hold writes at start past the first failed attempt, which with a store that hangs is the request deadline (60 seconds). A refusal holds them.

The hold at start ends once the replica is listed and the start is decided. A first snapshot, when one is needed, is taken after that, with writes allowed. The one long hold is a start with no working folder, which compares the database with the replica before it lets writes through.

## The working folder

Beside each database is a folder named `.<name>-litestream`, as with Go, holding the staged level-0 files under `ltx/0/`. Go and litestream-rs resume from each other's files there. Two files in it are litestream-rs's own, and Go neither reads nor removes them:

- `lsr-lock`: an exclusive lock held while a replicator runs, so two never stage into one folder. `lsr replicate` waits for it; the library refuses the second start. It excludes only on a local filesystem, and it does not exclude a Go process.
- `lsr-takeover`: one line saying this database may replicate above the bucket's files, through one transaction. `lsr trust-local` writes it, and so does a restore of a database a config names (`lsr restore`, and `restore-if-db-not-exists` at start), so a container that restores at start does not then stop. A restore to a path no config names, a backup or a copy, leaves only the database, as Go's does ([decisions/D215.md](decisions/D215.md)).

Keep the folder on the same durable disk as the database. On a fresh folder the replicator falls back to the page-by-page check above.

## Health and metrics

`Replicator::health()` returns a copy of the replicator's state: its phase, the degraded cause of each task, the last transaction synced and the last one replicated with their times, how long the oldest unreplicated commit has waited, whether writes are held and why, and counters. It takes one lock and does no I/O.

`lsr replicate` serves the same state when the config sets `addr`:

- `/healthz` returns JSON. It answers 200 while every database is running or degraded and still retrying, and 503 once one has stopped, failed to start, or tripped `if-replication-stops`.
- `/metrics` returns Prometheus text: Go's `litestream_*` series that the engine can count, plus `lsr_*` series. Alert on `lsr_unreplicated_age_seconds`, and on `lsr_replicated_txid` failing to advance while the application writes.

Degraded states and retry counts are not remembered across a restart, so a supervisor that restarts the process on a 503 hides them ([limitations.md](limitations.md)).