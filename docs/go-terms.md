# Go terms

A translation aid for reading this code beside Go Litestream, LTX and SQLite. Each row maps one of our terms to the word the other side uses for the same thing. Locations are in the pinned clones that `just upstream` fetches into `.upstream/` (`litestream` at 3e0fdc0, `ltx` at v0.5.3). Where a term has no counterpart it says so.

| Our term | Their word | Where |
|---|---|---|
| `CreatedAt`, `ListedTime::Created` | `FileInfo.CreatedAt` | `.upstream/ltx/ltx.go:596`; set at `.upstream/litestream/db.go:2421` |
| `litestream-timestamp` header (`CREATED_AT_HEADER`) | `MetadataKeyTimestamp` | `.upstream/litestream/s3/replica_client.go:55` |
| `NonZeroCreatedAt` | `CreatedAt` read from the metadata timestamp (zero when the writer recorded none), else `LastModified` | `.upstream/litestream/s3/replica_client.go:1759` |
| application (`AppCheckpoint`) | "other applications" | `.upstream/litestream/db.go:934` |
| other process (`OtherProcessMode`) | "by another process" | `.upstream/litestream/db.go:1840`, `:1895` |
| `OtherWriter` | none (Go has no check for a second replicator) | none |
| `UnrecognizedNames` | names `ParseFilename` rejects | `.upstream/ltx/ltx.go:467` |
| `NewestStagedFile` | "last LTX file" | `.upstream/litestream/db.go:1887` |
| `PositionSource`, `Position` | `ltx.Pos` | `.upstream/ltx/ltx.go:76` |
| `Txid` | `ltx.TXID` | `.upstream/ltx/ltx.go:137` |
| staged file (`staging`) | `ltxStagingFile` | `.upstream/litestream/db.go:1640` |
| `Task` | one monitor loop (database) | `.upstream/litestream/db.go:3322` |
| `Task` (replica uploads) | one monitor loop (replica) | `.upstream/litestream/replica.go:392` |
| `Task` (compaction, L0 retention) | one monitor loop per level | `.upstream/litestream/store.go:591`, `:658` |
| read lock (`ReadLockHold`) | "read lock" held by `rtx`, a long running read transaction | `.upstream/litestream/db.go:74`, `:900`, `:1287` |
| `release_read_lock` | "Release the read lock" | `.upstream/litestream/db.go:934` |
| read mark 0 | SQLite `aReadMark[0]` | SQLite `wal.c` (not in the clones) |
| backfill | SQLite `nBackfill`, reported as `backfill` by `wal_checkpoint` | `.upstream/litestream/db.go:1938` |
| WAL generation (`salt1`, `salt2`) | "WAL generation", the header salts | `.upstream/litestream/db.go:1246`, `:1953` |
| committed end, wal-index | `walIndexMaxFrame` ("last frame committed to the wal-index") | `.upstream/litestream/db.go:1927`, `:2216` |
| `CheckpointTrigger` | `MinCheckpointPageN`, `CheckpointInterval`, `TruncatePageN` | `.upstream/litestream/db.go:54`-`:60` |
| checkpoint `PASSIVE`, `RESTART`, `TRUNCATE` | the `mode` argument | `.upstream/litestream/db.go:2564` |
| `Barrier` | `barrierTx` | `.upstream/litestream/db.go:2660` |
| `CheckpointOutcome::LockBusy`, `Event::CheckpointDeferred` | "passive checkpoint skipped", reason "database busy" (after the busy timeout, which ours does not wait) | `.upstream/litestream/db.go:1529`, `:1559`, `:1584` |
| `PageIndex` | `PageIndexElem` map of the decoder | `.upstream/ltx/decoder.go:30` |
| `PageEncoding::Frame` | LZ4 frame (`lz4.NewReader`) | `.upstream/ltx/decoder.go:26`, `:53` |
| `PageEncoding::Block` | LZ4 block (`lz4.Compressor`) | `.upstream/ltx/encoder.go:42`, `:332` |
| snapshot level 9 | `SnapshotLevel` | `.upstream/litestream/compaction_level.go:9` |
| `RetentionHold` | none (Go's retention only deletes) | `.upstream/litestream/db.go:3122` (`EnforceSnapshotRetention`) |
| kept restore point | none | none |
| `PageChecksumTable` | none (Go checks no database page) | none |
| `StopCause` | none (Go returns an error and exits) | none |
| snapshot cause (`SnapshotCause`) | `info.reason` | `.upstream/litestream/db.go:1840` |
| journal files | SQLite `-wal`, `-shm`, `-journal` | SQLite file formats (not in the clones) |
| config key `host`, `host-key` | the SFTP backend's `host` and `host-key` (a URL host, not our "application") | `.upstream/litestream/cmd/litestream/main.go:1197` |
