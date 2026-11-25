# Known limitations

What to know before trusting litestream-rs with data, as of main on 2026-11-24. The short version: it is version 0.0.0, nobody has run it in production, and the defects below are open. [release-bar.md](release-bar.md) lists what must be true before first use; none of that bar is met yet.

Where an issue exists it is linked. Row ids such as BOOT-20 are rows of [BEHAVIOR.md](BEHAVIOR.md), which has the detail.

## Not yet done

- **No release.** Nothing is published, and the library's public interface will change before it is ([#318](https://github.com/emschwartz/litestream-rs/issues/318)).
- **The first start is expensive.** It reads and uploads the whole database twice, once as the first file and once as the first snapshot ([#340](https://github.com/emschwartz/litestream-rs/issues/340)). One measurement suggests the copy is limited by one CPU core, not by the disk ([#352](https://github.com/emschwartz/litestream-rs/issues/352)).
- **Writes wait for the first snapshot.** Under the default `hold-start`, writes are held at start until the first snapshot is staged, which grows with the database's size ([#351](https://github.com/emschwartz/litestream-rs/issues/351)). The fix, releasing writes once the start is decided, is being built.
- **No limit on read load.** A snapshot or a scheduled verify reads the whole database as fast as it can, in the application's own process when used as a library ([#315](https://github.com/emschwartz/litestream-rs/issues/315)).
- **Two memory terms grow with the database.** The page checksum table holds an entry per page ([#64](https://github.com/emschwartz/litestream-rs/issues/64)), and a replica's whole file listing is held in memory ([#68](https://github.com/emschwartz/litestream-rs/issues/68)). Memory and CPU on the hot paths are still being reduced ([#46](https://github.com/emschwartz/litestream-rs/issues/46)).
- **Restore reads one file at a time.** Restore time grows with the number of files in the chain times the store's round trip ([#326](https://github.com/emschwartz/litestream-rs/issues/326)), and it downloads every version of a page, not only the newest ([#44](https://github.com/emschwartz/litestream-rs/issues/44)).
- **Commits are noticed by polling** once a second, as in Go, so a commit waits up to a second before replication starts ([#43](https://github.com/emschwartz/litestream-rs/issues/43)).
- **Some credential sources are missing.** Assumed roles, IAM Identity Center and login sessions are refused by name ([#176](https://github.com/emschwartz/litestream-rs/issues/176)).
- **Some of Go's commands are missing:** `status`, `sync`, `list`, `info`, `reset`, `restore -f` and the control socket ([#177](https://github.com/emschwartz/litestream-rs/issues/177)). Storage other than S3 and a local directory, server-side encryption settings, storage classes and age encryption are refused at load ([config-keys.md](config-keys.md)).
- **Databases are found once, at start.** A `dir` entry is scanned once, so a database that appears later is not replicated until a restart, and a config change needs a restart ([#21](https://github.com/emschwartz/litestream-rs/issues/21)).
- **One thread and three SQLite connections per database**, which is fine for tens of databases and untested for hundreds ([#17](https://github.com/emschwartz/litestream-rs/issues/17)).
- **Linux and macOS only.** Nothing is built or tested on Windows.
- **Only verified SQLite builds.** The engine reads SQLite's private file layouts, so it refuses any build outside its list (SQLite 3.51.3 to 3.54.1 today).

## Known defects

- **Go Litestream and `lsr` writing one bucket path at once corrupt its history.** Go overwrites without a condition, so one transaction id can end with two contents. A later restore can exit 0 with a database that fails SQLite's integrity check, and `lsr` does not always report another writer ([#346](https://github.com/emschwartz/litestream-rs/issues/346)). Stop one completely before starting the other.
- **A lying disk plus a repeated write can fork history silently.** If a power loss drops two or more replicated transactions and the application then writes the same final transaction again, the check at start passes, and the replica's history differs from the database's until the next snapshot ([#12](https://github.com/emschwartz/litestream-rs/issues/12)). The test for it fails and is marked ignored.
- **A crash loop is expensive and partly invisible.** A start that cannot prove continuity takes a full snapshot, with no limit on how often, so a process killed every few seconds can upload the database again and again (row BOOT-20). `/healthz` and the hold on writes do not see a failure across restarts; the age gauge does ([#331](https://github.com/emschwartz/litestream-rs/issues/331)).
- **On Cloudflare R2, one stray file name can stop a level from listing.** R2 lists names that extend one another out of byte order, and the store refuses such a page. A replica only `lsr` and Go wrote is not affected ([#342](https://github.com/emschwartz/litestream-rs/issues/342)).
- **A stray connection that closes last costs a full snapshot.** If any connection closes after the replicator's, SQLite deletes the WAL, and the next start uploads the whole database with no warning beforehand ([#316](https://github.com/emschwartz/litestream-rs/issues/316)).
- **One crash-test failure is unexplained.** With the WAL sync removed, the real-SQLite crash rig once saw rows no commit made. It does not reproduce on main, and its cause was never found ([#94](https://github.com/emschwartz/litestream-rs/issues/94)).

## Untested conditions

- **Production.** No real application has run on it. The longest run on a real provider was 12 hours on Tigris, at older code, with a steady writer and no restarts. No soak has run on AWS S3 or R2.
- **Real providers, beyond one pass.** The conformance checks ran once each on AWS S3, R2 and Tigris on 2026-11-24, from one region, on buckets with no versioning and no lifecycle rule ([spikes.md](spikes.md)). Not run: a request cut mid-body against a real provider, versioned buckets, object lock, and a Tigris bucket read from a second region.
- **The newest rig has not finished a counted run.** The rig that kills and partitions the real binary against an S3 server landed on 2026-11-24, and its nightly 1,000-cycle run had not completed when this was written. The older power-loss rig for the real binary judges only the newest restore.
- **Filesystems where SQLite's locks or shared memory do not work** (NFS, some container file sharing). The read lock proves nothing there, and nothing refuses to start ([#332](https://github.com/emschwartz/litestream-rs/issues/332)).
- **Retention and compaction over real time windows.** No test lists what retention must keep and restores each item after a real delete job ([#333](https://github.com/emschwartz/litestream-rs/issues/333)).
- **Long or slow outages with the real binary.** Staging and then the WAL grow without an enforced bound while uploads fail. Tests hold a fault for tens of simulated seconds, not hours (row WORLD-34).
- **A paused process.** What writers see while the process is stopped holding its write lock is a guess (row WORLD-2). So is a second fault during recovery from a first (row WORLD-10), and a read-only remount or an exhausted file-descriptor limit (row WORLD-16).
- **A clock that steps forward.** The step is believed, so a restore to an instant inside that window is off by up to the step. No test covers it (guarantee G3 in BEHAVIOR.md).
- **Two failures inside SQLite's own checkpoint:** a failed sync of the database file (row CKPT-8) and a failed release of the write lock (row CKPT-6). Nobody knows what follows.
- **Upgrading `lsr` over its own working folder**, SQLCipher, a non-default VFS and WAL2 (rows WORLD-33 and WORLD-38).
- **Lag.** No test asserts how long a commit takes to reach the replica when all is well.
- **How many unknown defects the tests would catch.** The planted bugs are defects already fixed. The one blind measurement is old and small: the simulator alone found 7 of 22 bugs planted without a test written for them.
- **The simulator's SQLite is a model.** A wrong belief about SQLite that the engine and the model share is caught only by the real-SQLite rigs and the Go comparison. Mutation testing is complete for the LTX crate only ([mutants.md](mutants.md)), and one fuzz target still owes its 24-hour run ([#322](https://github.com/emschwartz/litestream-rs/issues/322)).
- **Outside review.** Every whole-project review so far was by a Claude model. No person outside the project has reviewed the code.

## Deliberate limits

These were decided, with the cost accepted. [differences-from-go.md](differences-from-go.md) has the comparison with Go.

- **One writer per bucket path, one replicator per working folder.** The folder's lock works only on a local filesystem and does not exclude a Go process. A second writer on the path is refused only where the store supports create-only writes; elsewhere `lsr` runs as Go does and says so ([decisions/D079.md](decisions/D079.md)).
- **A store whose listings lag its writes is unsupported.** A restore inside the lag can succeed with an older state. A Tigris global or dual-region bucket read from another region is such a store ([decisions/D173.md](decisions/D173.md)).
- **No file certifies the one before it.** Files carry no running database checksum, as Go's default, so a chain of files that each verify and together are wrong restores without error (row REST-9). The scheduled verify (`validation.interval`, off by default) compares the replica with the live database and is the check for this ([#14](https://github.com/emschwartz/litestream-rs/issues/14)).
- **A page damaged on disk while the process was down is trusted** on its first read after a start, and can be uploaded ([#10](https://github.com/emschwartz/litestream-rs/issues/10)).
- **A bucket rolled back while the replicator is down is taken as the bucket.** A hole below the newest file is not watched: a restore fails with `Gap` until the next snapshot, up to 24 hours by default ([decisions/D173.md](decisions/D173.md)).
- **Nothing but one small record is remembered across a restart.** Retry counts and degraded states start from zero, as in Go ([decisions/D174.md](decisions/D174.md)).
- **Files waiting in the working folder are uploaded as they are.** A folder put back from a backup that does not match the database uploads the other history's files ([decisions/D175.md](decisions/D175.md)).
- **It stops where Go carries on.** Restoring on a new host while the old host is still uploading, then starting replication, stops with `other_writer`, and under `hold-start` the application waits for a person. That is the design, and a rolling deploy can meet it.
- **Point-in-time restore is as fine as the files that remain.** Single transactions older than the level-0 retention (5 minutes by default) cannot be restored once merged. A commit is dated by when it was staged, so commits made during an outage are dated at catch-up.
- **A replica holds one page size.** After a page-size change the way on is a new bucket path.
- **TRUNCATE checkpoints are off by default.** With `app-readers: none` each one that empties the WAL is followed by a copy of the whole database ([decisions/D190.md](decisions/D190.md), [#334](https://github.com/emschwartz/litestream-rs/issues/334)).
- **An upload abandoned by a kill is aborted at the next start, not before.** Set a lifecycle rule on the bucket to abort incomplete uploads as the backstop ([#20](https://github.com/emschwartz/litestream-rs/issues/20)).
- **The engine thread runs on a fixed stack.** Its size comes from a measured peak, and the land gate runs the tests on half of it. An overflow would end the whole process, the application included, and nothing can catch it ([#358](https://github.com/emschwartz/litestream-rs/pull/358)).
- **`-exec` exits 1 when the child fails**, as Go does, not with the child's own code ([#41](https://github.com/emschwartz/litestream-rs/issues/41)).
