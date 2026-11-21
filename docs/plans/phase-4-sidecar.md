# Phase 4 (the sidecar): implementation plan

**Goal:** a `litestream replicate`-style daemon over the phase 3 library, so a host that cannot embed Rust, or runs Go's sidecar today, switches by replacing a binary: same config file, same meta directory, same replica, no snapshot when the WAL is intact, and a tested path back to Go.

**Spec:** `docs/PLAN.md` (section 1 "Not in v1"; section 3; section 6 host integration; phase 4; D4, D6, D7). Rules: `AGENTS.md`. Research: `docs/notes/pure-rust-pin.md` section 4B (deployment B, a separate process). Parked work: `docs/proposals/beyond-go.md`. Go behaviour below is cited from a recent upstream checkout at `/home/user/litestream`, **not the pinned 041a8c3**; each citation is rechecked against `.upstream/litestream` in the task that relies on it, and a line that moved is corrected in the PR.

Status: draft. Starts after phase 3's public API (task 6, `lsr-replica`) lands; tasks 8 and 9 also need phase 3 tasks 4 and 7. Its decisions are ruled (see "Rulings").

## What Go's sidecar does (recent checkout)

- **One process, many databases, one replica each.** `dbs:` is a list (`cmd/litestream/main.go:247`); each entry has exactly one `replica` (or a one-element deprecated `replicas`), more is an error (`main.go:641-648`). All databases share one `Store` (`replicate.go:230-231`). A `dir` entry with `pattern`, `recursive` and `watch` expands to many databases (`main.go:585-589`, `replicate.go:205-228`), each with a replica path and meta path derived from its relative path (`main.go:716-850`); `watch` adds and removes databases at run time through fsnotify (`directory_watcher.go:12`, `:40`).
- **Config file.** YAML parsed by `gopkg.in/yaml.v2` with plain `yaml.Unmarshal` (`main.go:22`, `:529`), so **unknown keys are silently ignored**. Default path `/etc/litestream.yml` (`main_notwindows.go:12`), overridden by `LITESTREAM_CONFIG` (`main.go:1856-1861`) or `-config`. `$VAR` and `${VAR}` are expanded over the whole file before parsing, `$PID` is the process id, and `-no-expand-env` turns it off (`main.go:514-521`, `:1863-1866`). Replica settings may sit at the top level and are copied into every replica that does not set them (`main.go:216`, `:287-297`, `:1024-1162`). Paths get `~` expansion and lose a `sqlite://` or `sqlite3://` prefix (`main.go:1875-1916`). Defaults: `main.go:300-323`; validation (no zero or negative intervals): `main.go:326-442`. `LITESTREAM_ACCESS_KEY_ID` and `LITESTREAM_SECRET_ACCESS_KEY` are copied to the `AWS_` names when those are unset (`main.go:1812-1823`). A replica type outside the eight Go knows is an error (`main.go:1199-1233`); `age` settings are refused (`main.go:1185-1187`).
- **Command line.** `litestream replicate [flags]` with a config, or `litestream replicate [flags] DB_PATH REPLICA_URL` without one (`replicate.go:76-136`). Flags: `-config`, `-no-expand-env`, `-exec`, `-log-level`, `-restore-if-db-not-exists`, `-once`, `-force-snapshot`, `-enforce-retention` (`replicate.go:62-69`), with the combinations refused at `replicate.go:159-167`.
- **Signals.** SIGINT and SIGTERM only (`main_notwindows.go:22-25`); no reload. On either, the signal is forwarded to the `-exec` child, Go waits for it, then closes every database (`main.go:140-160`). Each close runs a final WAL sync and then retries the replica sync every `shutdown-sync-interval` (500 ms) until `shutdown-sync-timeout` (30 s) (`db.go:458-540`, `db.go:36-37`). Any other signal has Go's runtime default.
- **`-exec`.** The string is split with go-shellwords (`replicate.go:354`), run with the parent's environment, stdout and stderr (`replicate.go:359-366`). The child exiting ends the sidecar (`main.go:134-139`); a signal error from the child is not reported as a failure (`main.go:150`); any error exits 1 (`main.go:82-87`). The child starts after `Store.Open` (`replicate.go:280`, `:353`), but `Store.Open` does not take the read lock: `DB.Open` only starts the monitor goroutine (`db.go:419-450`), whose first sync takes it later. So Go's child can write before Go's pin exists.
- **Database setup.** Go's own connection uses `wal_autocheckpoint(0)` and `PERSIST_WAL` (`db.go:626-636`), sets `journal_mode = wal` on the database (`db.go:655-662`) and creates `_litestream_seq` and `_litestream_lock` (`db.go:666-675`). A database file that does not exist yet is skipped until it appears (`db.go:611-614`). It cannot set anything on the host's connections.
- **The host checkpointing anyway** is detected by its effects: a WAL shorter than the synced offset (`db.go:1138-1167`), a salt reset (`db.go:1186-1204`), a last page missing from the last LTX file (`db.go:1215`), a FULL or RESTART checkpoint (`db.go:1231-1234`). Each ends in a snapshot or, after a sync to the WAL end, a continue (`db.go:1144-1163`).
- **Metrics and health.** `addr` serves Prometheus `/metrics` (`replicate.go:334-350`); the listener binds the address as given, so `:9090` is every interface although the log line says localhost (`replicate.go:337-346`). Series: `litestream_db_size`, `_wal_size`, `_total_wal_bytes`, `_txid`, `_sync_count`, `_sync_error_count`, `_sync_seconds` (label `db`), `_checkpoint_count`, `_checkpoint_error_count`, `_checkpoint_seconds` (labels `db`, `mode`), `_compaction_verify_error_count` (`db.go:2258-2310`), `litestream_replica_operation_total`, `_bytes`, `_duration_seconds`, `_errors_total` (labels `replica_type`, `operation`, and `code`), `litestream_l0_retention_files_total` (`internal/internal.go:162-187`). pprof rides on the same mux (`replicate.go:11`). There is no health endpoint; a stalled replica shows only as a `litestream_txid` that stops moving.
- **Logging.** `log/slog` to stdout, or stderr with `logging.stderr`; `logging.type` `text` or `json`; level from `logging.level`, `LOG_LEVEL`, then `-log-level` (`main.go:565-573`, `:1936-1970`, `replicate.go:85-95`).

## What phase 3 hands over

- `lsr_replica::Replicator` (phase 3 task 6): `start` returns once the read transaction is held; `final_sync(deadline) -> FinalSync`; `health() -> Health`; `set_readers(Readers)`; `restore`, `verify`. Drop does no I/O on the caller's thread.
- `Responses` with `GO_LIKE` and `STRICT`, lanes and the degraded states (phase 3 task 10, ruling P7).
- `lsr replicate <db> <replica-url>` in `crates/cli`, one database, stopped by SIGTERM (phase 3 task 6). Phase 4 grows it; it keeps working unchanged.
- The real-SQLite fault suite, generic over the backend (phase 3 task 4), and its outside-checkpoint cases in `crates/compat/tests/live_source.rs`: `outside_restart_against_pin_above_zero`, `outside_truncate_against_pin_above_zero` (BUSY, WAL unchanged, next sync incremental), `pin_on_zero_one_restart` (a snapshot, `GenerationChanged`), `pin_on_zero_proven_restart_continues_in_the_new_generation` (`WalRestarted`, no snapshot); and task 4's `the_pin_survives_the_hosts_last_connection_closing` and "outside PASSIVE, RESTART and TRUNCATE while pinned and while not".
- The Go process harness (`crates/compat/src/replicator.rs`, `scripts/build-go-litestream.sh`) and the alternation test (phase 3 task 7).
- The upstream-test ledger (phase 3 task 8), where `cmd/litestream` tests carry `reason` entries phase 4 turns into counterparts.

## How the sidecar differs from the library in a host

**The pin.** Ruling P0 stands: a SQLite read transaction on the sidecar's own connection holds the pin. POSIX locks conflict across processes, so a host process that opens and closes the database or `-shm` drops only its own locks, never the sidecar's (research note 4B), and the host-side rule of section 6 ("no `std::fs` open of the database files") no longer binds the host. It still binds **the sidecar's own process**: a descriptor on the database or `-shm` that the sidecar opens and closes drops the sidecar's own locks, the pin's read-mark lock included. So phase 3's global constraint applies to every sidecar crate, not only `lsr-sqlite`: the directory scan (O5) reads a candidate's header only before any actor opens that file, and nothing in the daemon opens a file an actor has open. The pure-Rust pin (#9) stays deferred; 4B makes it simpler here (plain `F_SETLK` suffices), not needed.

**The host's connection settings.** The sidecar cannot set `PERSIST_WAL` or `wal_autocheckpoint = 0` on the host's connections (`app_connection` is in-process only), and it cannot read them. It cannot call `set_readers` either, so `Readers` is `Unknown` (P3: never TRUNCATE) unless O4 rules otherwise. What follows when the host keeps SQLite's defaults:
- *Autocheckpoint (PASSIVE every 1000 pages by default).* Backfill only copies frames into the database file; it never overwrites the WAL, and it stops at the pin's read mark. Harmless to the replica. It is detected (task 7) as `nBackfill` rising with no checkpoint of the sidecar's own in between, from the `CommittedRead { end, backfilled }` the actor already reads each poll.
- *A WAL restart under a pin on mark 1 or more* (the host's RESTART or TRUNCATE, or its next writer after a full backfill) is refused by SQLite: BUSY, salts and length unchanged, the next sync incremental (`outside_restart_against_pin_above_zero`, `outside_truncate_against_pin_above_zero`).
- *A restart under a pin on mark 0* (the WAL was fully backfilled when the pin was retaken) is allowed by SQLite. With the sidecar synced to the end in that epoch it is followed with no snapshot (`pin_on_zero_proven_restart_continues_in_the_new_generation`); otherwise it is a snapshot with cause `GenerationChanged` (`pin_on_zero_one_restart`); F1's tripwire (`PinViolated`) covers the rest. Host checkpoints make mark 0 more frequent than in process; task 0 measures by how much.
- *No `PERSIST_WAL`.* While the sidecar runs, its open connection keeps the host's last close from deleting the WAL (`the_pin_survives_the_hosts_last_connection_closing`, now across processes). While the sidecar is down, the host's last close checkpoints and deletes `-wal`; the next boot snapshots with cause `WalMissing`. A WAL unlinked mid-run is `WalUnlinked` (phase 3 task 4).
- *Any checkpoint while the sidecar is down* is classified at boot by `recover` as in phase 3: continue when the WAL still matches, a snapshot with its cause otherwise.

None of these is silent: each snapshot carries its `SnapshotCause`, and task 7 adds a host-checkpoint notice to `health()`. None is a degraded state: replication stays correct, the cost is snapshots. The plan does not add a way to stop on them (Go does not), and the notice's text tells the operator to set `wal_autocheckpoint = 0` and `PERSIST_WAL` in the host to avoid the cost.

**Shared fate** (PLAN section 7) changes: an OOM kill of the host no longer stops replication, and the sidecar's own crash leaves the host serving with an unpinned WAL until restart.

## Out of scope (Go's sidecar does these; v1 does not)

Each is refused at config load when set to anything but its inert value (O1), never ignored.
- Replica backends other than S3 and a local directory: `gs`, `abs`, `sftp`, `webdav`, `nats`, `oss` (`main.go:1199-1233`). PLAN section 1.
- The control socket and the `start` and `stop` commands (`socket:`, `server.go:16-30`, `:57-77`; `replicate.go:285-293`). PLAN section 1.
- Heartbeat (`heartbeat-url`, `heartbeat-interval`; `replicate.go:259-265`). PLAN section 1.
- Runtime auto-recover (`auto-recover: true`; `main.go:959`, `:1194-1196`). PLAN section 1: suspected lineage-fork bug.
- The VFS read replica: a separate Go binary configured by environment variables (`cmd/litestream-vfs/main.go:46-102`), not by `litestream.yml`, so there is no key to refuse; the Go VFS stays in use. PLAN section 1.
- The v0.3 `generations/` layout and multi-writer leases. PLAN section 1.
- The MCP server (`mcp-addr`; `replicate.go:178-184`), pprof (`replicate.go:11`), the Windows service (`main_windows.go:23-44`), periodic validation (`validation.interval`, `validation-interval`; `store.go:193`, `:748`) and `verify-compaction` (`replicate.go:253-255`). Not in section 1's list but not v1 work; `lsr verify` is the validation v1 offers.
- Go's other subcommands (`databases`, `ltx`, `reset`, `restore`, `status`, `version`; `main.go:116-180`). `lsr restore` and `lsr verify` keep their own flags. A container entrypoint that runs `litestream restore -if-replica-exists` is not a drop-in in v1; `-restore-if-db-not-exists` on `replicate` is (task 3).
- Byte-identical log lines and Go's exact error texts. Keys, levels and formats match (task 3); messages do not.

## Global constraints

The constraints of the phase 1 to 3 plans apply unchanged. In addition:
- **No descriptor on a database or `-shm` that an actor has open is opened and closed by any sidecar code** (see "The pin" above). `crates/sidecar/clippy.toml` disallows `std::fs::File::open`, `OpenOptions::open` and `std::fs::read`; the directory scan's header read is the one reasoned `#[expect]`, and it runs before the actors start.
- **No async runtime.** The daemon is threads: one per database (the actor, as phase 3), one for signals, one for the HTTP listener, one waiting on the `-exec` child. Every channel between them is bounded.
- **D6 holds for the daemon.** No file in the meta directory or the replica that Go does not write. The meta path default is Go's (`.<name>-litestream` beside the database, `db.go:193`, `litestream.go:20`), so a Go meta directory is the Rust one.
- **Never silently wrong about config.** A key that asks for behaviour v1 does not have is an error naming the key path, at load, before any database is touched (O1).
- New dependencies, each named with its licence in its PR: a YAML parser (task 1), and a signal crate (task 3) because `unsafe` is denied outside `lsr-sqlite`. Nothing else; the HTTP listener and the Prometheus text are written by hand (O2).

## Review focus

1. A Go config key that requests behaviour v1 lacks and is accepted anyway (the replica is then not what the operator configured).
2. A sidecar code path that opens and closes a database or `-shm` descriptor while an actor holds the pin.
3. Shutdown: exit 0 with a committed transaction not on the replica, or a signal that ends the process without `final_sync`.
4. The `-exec` child started before every existing database's pin is held.
5. A Go meta directory or Go config that boots with a snapshot when the WAL is intact.
6. Health that reports healthy while a lane is degraded or stopped.

## Task order

0 (spikes) first. 1 (config parser) and 7 (host-checkpoint observer, pure core) in parallel. 2 (config to library) after 1 and O1, O3 to O5. 3 (the daemon) after 2 and phase 3 task 6. 4 (`-exec`) and 5 (health and metrics, after O2 and 7) after 3. 8 (fault suite across processes) after 3 and phase 3 task 4. 9 (alternation and the Go-config start, sidecar to sidecar) after 3, 4 and phase 3 task 7. 10 (ledger) any time, closes last.

## Open decisions (the owner's)

None open. O1 to O6 were ruled by the owner on 2026-11-20, each as recommended; the options considered are kept under "Decisions and their options" at the end of this plan.

## Rulings

All ruled by the owner on 2026-11-20, as recommended.

- **R1 (O1): Go's config format, strict on meaning.** Keys v1 implements are honoured; Go keys for features v1 lacks are accepted only at their off value, else an error naming the key path; Go tuning knobs with no Rust counterpart are accepted with one warning; keys Go does not know are refused with a "did you mean". `lsr config check` shows the result before a switch.
- **R2 (O2): health on Go's `addr` listener.** Go's `/metrics` series where v1 has the value, new series under their own prefix, and `/healthz` (200 when running or degraded, 503 when stopped), on a hand-written server with no new dependency.
- **R3 (O3): Rust-only settings under one top-level key in `litestream.yml`,** which Go ignores, so rolling back is a binary swap.
- **R4 (O4): no TRUNCATE by default;** the operator opts in by stating that the host holds no long readers.
- **R5 (O5): directory entries scanned once at start, as Go derives their paths; `watch: true` refused in v1.**
- **R6 (O6): Go's signals, with SIGHUP as a clean stop with a final sync, and a restart as the reload.**
- **R7: three departures from Go, confirmed.** One shutdown deadline shared by all databases (Go drains them one after another, `store.go:204-220`); a database that fails to start leaves the others running (Go exits); a child process killed by a signal the sidecar forwarded counts as a clean exit, as in Go (`main.go:150`).

---

### Task 0: spikes (branch `sidecar-spikes`)

**Files:** `docs/spikes.md`, section "Phase 4 spikes"; scratch under `crates/sidecar/examples/`, deleted before landing unless a test keeps it.

- **S6, YAML fidelity.** Parse every config literal in upstream's `cmd/litestream/main_test.go` and `etc/litestream.yml` with each candidate parser (`saphyr` and `yaml-rust2`, both MIT or Apache-2.0, into a node tree; `serde_yaml` is archived). List every difference from `yaml.v2`: YAML 1.1 booleans (`yes`, `on`), octal permissions (`0600`), durations (`time.ParseDuration` strings, a bare integer as nanoseconds), merge keys, duplicate keys. The answer picks task 1's parser.
- **S7, the host's autocheckpoint against a sidecar pin.** A host process at SQLite's defaults (autocheckpoint 1000, no `PERSIST_WAL`) writes for 10 minutes while `lsr replicate` (phase 3 task 6) runs in another process. Count snapshots by cause, mark-0 retakes, outside backfills. Repeat with the host at `wal_autocheckpoint = 0`. The numbers go into task 7's notice text and decide whether O4 needs revisiting.
  **Answered:** `docs/spikes.md`, "S7: the host's automatic checkpoints against a pin in another process", probe `crates/sqlite/tests/sidecar_pin.rs`. Measured with the engine's actor over the real pin and the host in a child process rather than a ten-minute `lsr replicate` run. No snapshot either way; above read mark 0 the host's PASSIVE backfills exactly to the pin's mark and the WAL never restarts; on mark 0 it backfills nothing (so task 7 cannot see it in `nBackfill`), and the one restart is the first write after the replicator's own checkpoint. O4 stands.

### Task 1: `lsr-config`, Go's config file (branch `go-config`)

**Files:** create `crates/config/` (package `lsr-config`; `src/{lib,expand,yaml,keys,duration,size,error}.rs`, `tests/`); `crates/cli` gains `lsr config check <file>`.

**Produces:**
- `GoConfig::load(path, Expand) -> Result<GoConfig, ConfigError>` and `GoConfig::parse(text, Expand, env: &dyn Env)`: the whole Go file as typed values, `Option` where Go uses a pointer, defaults from `main.go:300-323`, top-level replica settings propagated as `main.go:287-297` and `:1024-1162`, validation as `main.go:326-442` (a zero or negative interval is an error, as in Go).
- `Expand`: Go's `os.Expand` over the raw text with `$PID` (`main.go:514-521`); off for `-no-expand-env`. Path expansion as `main.go:1875-1916`. Durations as `time.ParseDuration`; byte sizes as `ParseByteSize` (`main.go:930`).
- `KeyClass { Implemented, Inert, NoEffect, Unknown }` per key path (O1), and `Report { warnings, errors }`: every error names the key path (`dbs[0].replica.type`) and never prints a value from a key that holds a secret (the `SecretString` rule of `lsr-store-s3`).
- `lsr config check <file>`: the report, exit 1 on any error; no database or bucket is touched.

**Semantics:** the parser walks a node tree, never `serde` defaults, so a key's absence, an empty section and a zero are three different values (Go restores defaults after an empty section, `main.go:533-545`; copy that). Duplicate keys: Go's `yaml.v2` behaviour per S6. The O3 key is parsed here into `RustSettings`.

**Tests (failing first):** a table transcribed from every config case in upstream's `main_test.go`, each with Go's expected values or Go's error; `etc/litestream.yml` uncommented line by line; each class of O1 with one key (`type: gs` refused, `socket.enabled: false` accepted, `busy-timeout` warned, `sync_interval` refused with the suggestion); `$VAR`, `${VAR}`, `$PID`, an unset variable, `-no-expand-env`; a secret never appears in an error (property: random secret strings, no error text contains them); proptest round trips for durations and sizes against Go's grammar.

**As built (S6):** answered in `docs/spikes.md`, "Phase 4 spikes": `saphyr-parser` 0.3.4 as an event parser, yaml.v2's resolution ported.

**As built (task 1):** `crates/config` is `src/{lib,config,build,walk,keys,value,yaml,expand,duration,size,error}.rs` and one test binary, `tests/config/` (`go`, `classes`, `fidelity`, `env`, `upstream`), with upstream's `etc/litestream.yml` copied to `tests/data/`. `lsr config check` is not added (another branch owns `crates/cli`); `lsr_config::check(path) -> Report` and `check_with`/`check_text` are what it calls. Differences from the text above:
- `GoConfig::parse(text, Expand, &dyn Env)` and `GoConfig::load(path, Expand, &dyn Env)` return `Loaded { config, report }`; a refusal is `ConfigError`, which holds the whole `Report` (every problem, not the first). `Report { keys, warnings, errors }` lists every key the file sets with its `KeyClass`. `Env` also supplies the pid, the home directory and the current directory, so tests are deterministic.
- The typed config holds only implemented keys (inert keys are checked and dropped, no-effect keys warned and dropped); fields are private behind getters, durations are `GoDuration` (signed nanoseconds, as Go), and the replicator-facing defaults Go applies at database creation stay `None` for task 2.
- Go at 041a8c3 already warns on unknown and duplicate keys (`UnmarshalStrict`, a diagnostic second pass); v1 refuses both, the duplicate naming both lines.
- Classification beyond the plan's lists: `retention.enabled` is implemented (core gained `RetentionMode`); `sse-*` and `storage-class` are inert, not no-effect (ignoring them changes what is stored); `max-sync-ltx-files` and `max-sync-wal-bytes` are implemented (core's `UploadConfig` and `SyncConfig`); `shutdown-timeout`, `logging.source`, `name`, `sign-accept-encoding`, `put-object-content-md5` and a top-level `part-size` (Go does not copy it into replicas) are no-effect; keys only other backends read are accepted at any value, since those backends are refused; `logging.type: pretty` is refused as not in v1, and an unknown `logging.level` or `LOG_LEVEL` is refused where Go falls back to info.
- What Go refuses only when it starts a database (`replica` with `replicas`, two replicas, a URL in `path`, an unknown or out-of-scope replica type) is refused at load. Paths: database `path` is expanded at load as Go's `ParseConfig` does; `meta-path`, `meta-dir`, `dir` and replica paths stay as written, with `expand_path` exported for task 2.
- `truncate-page-n` other than 0 (Go's 0 means its default) is refused without `litestream-rs: {host-readers: none}`, and a load without the claim warns once that v1 never truncates.
- The secret rule: type errors on keys marked secret never carry the value; a property test puts random secrets in secret keys of broken files and checks no report text contains them.

### Task 2: from config to replicators (branch `config-to-replica`)

**Files:** `crates/config/src/map.rs`; `crates/replica/src/config.rs` (constructors only).

**Produces:** `fn plan(config: &GoConfig) -> Result<Vec<DbPlan>, MapError>`: per database its path, meta path (Go's default `db.go:193`, or `meta-path`), `S3Config` or a directory store, `ActorConfig` (checkpoint thresholds onto `CheckpointConfig`, Go's 0 mapped to `None` where Go treats 0 as off, each mapping citing the Go line), `Responses` from O3, `Readers` from O4, `restore_if_missing`. `dir` entries expanded per O5. Two entries resolving to the same database path, or two databases on the same replica prefix, are an error (Go dedups the first, `store.go:234-242`; the second is a second writer on one prefix, which D7 stops on anyway).

**Semantics:** credentials as Go: the key's value, else `LITESTREAM_*` copied to `AWS_*`, else `AWS_*` (`main.go:1812-1823`). The S3 URL forms of `lsr-store-s3::S3Config::from_url` must accept every query parameter Go's `NewS3ReplicaClientFromConfig` reads (`main.go:1272-1480`); one not supported is O1 class (b).

**Tests:** one per mapped key against the Go line it copies; a `dir` tree with nested databases yields Go's replica URLs and meta paths (the table from upstream's directory tests); duplicate paths and shared prefixes refused; a Go meta directory path is the one Rust computes.

### Task 3: the daemon (branch `sidecar`)

**Files:** create `crates/sidecar/` (package `lsr-sidecar`; `src/{lib,daemon,signals,logging,once,error}.rs`, `clippy.toml`, `tests/`); `crates/cli` routes `lsr replicate` to it. The phase 3 two-argument form keeps working.

**Produces:**
- Go's `replicate` command line, Go's flag syntax (`-flag`, `--flag`, `-flag=value`, `-flag value`), the flags and refusals of `replicate.go:62-167`.
- `Daemon::run(plan) -> ExitCode`: `-restore-if-db-not-exists` first, as `replicate.go:443-489` (no backup is not an error); then one `Replicator::start` per database, in config order; a database file that does not exist yet is retried every `monitor-interval` and reported as `WaitingForDatabase` in health (Go `db.go:611-614`). `journal_mode = wal` is set if the file is not in WAL mode (Go `db.go:655-662`; in `lsr-sqlite`'s open if phase 3 did not already).
- Signals: SIGINT, SIGTERM and (O6) SIGHUP start shutdown: every database's `final_sync` with the deadline `now + shutdown-sync-timeout`, polled together (one deadline, not one per database in turn, so N databases do not take N times the budget; Go closes them in turn, `store.go:204-220`, which is the obvious thing to improve). A second signal during shutdown is ignored, as Go.
- Exit: 0 when every database is `Drained`; 1 otherwise, after one log line per database naming `DeadlineReached { synced, committed }` or the stop cause.
- `-once` (with `-force-snapshot` and `-enforce-retention`): one sync and upload per database, a snapshot with `SnapshotCause::OperatorRequested`, a retention pass, then exit, as `replicate.go:375-416`.
- Logging: `logging.level`, `LOG_LEVEL`, `-log-level` with Go's precedence; `text` (logfmt) or `json` lines; stdout or stderr. The `EventSink` writes one line per event with the database path; `SnapshotCause` and degraded causes are fields, so the grep upstream users do for forced snapshots keeps working on the key.

**Semantics:** a signal-handling crate (`signal-hook`, MIT or Apache-2.0) is the only new dependency; its thread forwards into a bounded channel. No panic leaves a database thread (phase 3 task 6); a database that fails to start logs and is reported stopped, the others keep running (Go returns an error and exits, `replicate.go:220-227`: a host with five databases should not lose backup of four because one path is wrong, and the exit code still says so at shutdown).

**Tests:** start three databases from one config, write to each, SIGTERM, exit 0 and each replica restores equal; a deadline of zero with work pending exits 1 naming the database; SIGHUP drains (O6); a missing database is picked up when created; one bad database does not stop the others; `-once` and its refusals; `-restore-if-db-not-exists` with and without a backup; log lines parse as logfmt and JSON with the documented keys.

### Task 4: `-exec` (branch `sidecar-exec`)

**Files:** `crates/sidecar/src/exec.rs`, `tests/exec.rs`.

**Semantics:** the string split as go-shellwords' defaults (no env or backtick expansion), by a hand-written splitter with a table of go-shellwords cases, or the `shell-words` crate (MIT or Apache-2.0) if the table passes; parent environment, stdin null, stdout and stderr inherited (`replicate.go:359-362`). **The child starts after every database that exists has its pin held** (every `Replicator::start` returned), unlike Go (see above), so section 6's "replicator started before any connection pool opens" holds for an `-exec` host. A database that does not exist yet does not delay the child; its first sync is then `FirstSync`. A signal is forwarded to the child, the daemon waits for it, then shuts down (`main.go:140-153`); the child exiting starts shutdown (`main.go:134-139`). Exit code: 0 when the child exited 0 and every database drained; 1 otherwise, with a child that died by the forwarded signal counted as clean (`main.go:150`).

**Tests:** the child observes the pin (a TRUNCATE from the child at its first instruction reports BUSY); SIGTERM reaches the child and the daemon drains after it exits; the child exiting 0 and 3; a quoted argument table against go-shellwords.

### Task 5: health and metrics (branch `sidecar-health`; after O2)

**Files:** `crates/sidecar/src/{http,metrics,health}.rs`, `tests/http.rs`.

**Produces (O2 option A):**
- `/metrics`, Prometheus text format: Go's series with Go's labels where v1 has the value (`litestream_txid`, `_db_size`, `_wal_size`, `_total_wal_bytes`, `_sync_count`, `_sync_error_count`, `_sync_seconds`, `_checkpoint_count`, `_checkpoint_error_count`, `_checkpoint_seconds`, and the four `litestream_replica_operation_*` series with `replica_type="s3"` or `"file"`), each listed in the module doc with the Go line; one not emitted is listed with why. New series: `lsr_degraded{db,cause,lane}` (1 while degraded), `lsr_stopped{db,cause}`, `lsr_synced_age_seconds{db}`, `lsr_snapshot_total{db,cause}`, `lsr_host_checkpoint_total{db}` (task 7).
- `/healthz`, JSON, D7's health output: per database the path, phase, last synced txid and its age, every degraded state `{cause, lane, since}`, the stop cause, the host-checkpoint notice, `WaitingForDatabase`. Status 200 when no database is stopped or not started (degraded is 200: D7 keeps trying, and an orchestrator that restarts on 503 would only repeat the boot); 503 otherwise. The body says which.
- Bound as Go: the address as given (`replicate.go:346`), with the log line naming the real bind, not localhost.

**Semantics:** each request reads `health()` snapshots, a copy of one `Arc` per database, never a lock across I/O; at most 8 connections, 8 KiB of request, a 5 s read deadline; anything but `GET` of those paths is 404 or 405.

**Tests:** a degraded lane (phase 3 task 10's fault) appears in both outputs with its cause and lane, and clears; a stop gives 503; the metrics parse with a Prometheus text parser written for the test and carry Go's names and labels; a request over the bound is refused without growing memory.

### Task 6: deferred

Kept free for O6 option B (reload) if ruled; otherwise unused, so task numbers stay stable.

### Task 7: the host-checkpoint observer (branch `host-checkpoints`; pure core)

**Measured first (spike S7, `docs/spikes.md`):** a host checkpoint is visible as a rise in `nBackfill` only while the pin is on a read mark above 0. On read mark 0, SQLite's checkpoint needs that mark exclusively, fails, and reports OK to a PASSIVE caller, leaving no trace in the WAL index. So the observer cannot rely on `nBackfill` alone. Either accept that it sees only the mark-above-0 half and say so in the notice, or detect the other half another way (for example, WAL growth past `wal_autocheckpoint` pages with no backfill, which means the host is attempting checkpoints that cannot run). The notice should also say what the cost is: past 1,000 frames every host commit runs a checkpoint attempt that does nothing until the replicator checkpoints, and the WAL keeps growing until then.

**Files:** `crates/core/src/host.rs` and its tests; `actor.rs` (calls it each poll); `crates/replica/src/health.rs` (the notice); a scenario in `crates/sim/src/scenario/faults.rs`.

**Produces:** `fn observe(prev: CommittedRead, now: CommittedRead, ours: Option<Checkpointed>) -> Option<HostCheckpoint>`: `Backfilled { from, to }` when `backfilled` rose with no checkpoint of ours between the reads; `Restarted` when the salts changed with no checkpoint of ours. An event per observation and `Health::host_checkpoints { count, last }`. The notice is not a `Fault` and not a degraded state (see "The host's connection settings"); it changes no decision.

**Tests:** proptest over the simulator's SQLite model with a stranger checkpointing: every outside backfill or restart is observed, and none of the actor's own is; the simulator scenario `host_autocheckpoints` with the oracle holding and the count matching; in-process, the library with a host that keeps `wal_autocheckpoint = 0` sees a count of 0.

### Task 8: the fault suite across processes (branch `sidecar-faults`)

**Files:** `crates/sqlite/tests/` gains a placement axis in the phase 3 task 4 harness (`InProcess`, `SeparateProcess`); `crates/sidecar/tests/host.rs`.

**Semantics:** with `SeparateProcess`, the actor runs in a child process (`lsr replicate`) and the test process plays the host, its writes and its checkpoints from its own connections; the oracle reads the replica as before. Every phase 3 task 4 case runs in both placements unchanged; a case whose answer differs by placement states both answers in the suite's table (the expected one: none).

**Tests (new):** the host at SQLite's defaults (autocheckpoint on, no `PERSIST_WAL`) for every case of "The host's connection settings": PASSIVE by the host while pinned (no snapshot, notice counted); RESTART and TRUNCATE by the host against a pin above zero (BUSY, incremental); the mark-0 restart both ways (`GenerationChanged` or `WalRestarted`); the host's last connection closing while the sidecar runs (WAL kept) and while it is down (next boot `WalMissing`); the sidecar SIGKILLed mid-upload with the host still writing and checkpointing, then restarted: the oracle holds; the host process opening and closing the database file with `std::fs` while the sidecar is pinned: the pin holds (4B).

### Task 9: Go sidecar to Rust sidecar (branch `compat-sidecar`)

**Files:** `crates/compat/tests/sidecar.rs`; `crates/compat/src/replicator.rs` gains config-file mode for both binaries. Every test that runs Go is `#[ignore]` and runs under `just compat`.

**Tests:**
- **Go config, Go meta directory, no snapshot.** Go `litestream replicate -config litestream.yml` runs with three databases and a host process writing; stop Go; start `lsr replicate -config litestream.yml` with the same file unchanged; each database's next file is `tip + 1`, incremental, no snapshot cause; restores match. The config includes inert class (b) and class (c) keys, and the O3 key, which Go must ignore (checked for v0.5.16, v0.5.18 and Head).
- **The alternation test as sidecars.** Phase 3 task 7's test with both sides as config-driven processes and a separate host process that keeps writing (and, in a second run, autocheckpointing): at least 20 switches per run, SIGKILL at a random point from a printed seed; every switch boots with no snapshot cause when the host has autocheckpoint off, and with only causes from "The host's connection settings" when it has it on; a restore at every acknowledged txid matches. v0.5.18 and Head; the nightly workflow adds every release from v0.5.16.
- **Metrics carry over.** The same scrape against Go and Rust returns the same series names and labels for the series task 5 lists.

### Task 10: the ledger (branch `ledger-sidecar`)

**Files:** `docs/ledger.toml`.

**Semantics:** every `cmd/litestream` test with a `reason` entry from phase 3 is revisited: a counterpart in tasks 1 to 9, or a reason naming the out-of-scope line it falls under. The PR states the counts before and after.

## Decisions and their options

Kept for the record; the rulings above are what stands.


#### O1: the config file: Go's `litestream.yml` as is, or a stricter subset

Whatever is chosen, the file format, location, env expansion and defaults are Go's. The question is what happens to keys v1 does not implement, and to keys Go does not define.

Keys fall into four classes:
- (a) **Go knows it, v1 implements it**: `dbs[].path`, `meta-path`, `replica.url`/`type`/`path`/`bucket`/`region`/`endpoint`/`force-path-style`/`access-key-id`/`secret-access-key`/`part-size`, `sync-interval`, `monitor-interval`, `checkpoint-interval`, `min-checkpoint-page-count`, `truncate-page-n` (subject to O4), `snapshot.interval`/`retention`, `levels`, `l0-retention`, `l0-retention-check-interval`, `shutdown-sync-timeout`, `addr`, `exec`, `logging`, `restore-if-db-not-exists`.
- (b) **Go knows it, v1 does not do it**: the out-of-scope list above (non-S3 types and their keys, `socket`, `heartbeat-*`, `auto-recover`, `mcp-addr`, `validation`, `verify-compaction`, `age`, `dir` with `watch` under O5).
- (c) **Go knows it, a tuning knob with no counterpart in Rust**: `busy-timeout` (both connections run with busy timeout zero, phase 3 task 1), `shutdown-sync-interval` (the actor's own retry policy), `concurrency` (uploads are sequential), `sign-payload`, `require-content-md5` (Content-MD5 is always sent, phase 2), `sse-*`, `skip-verify`.
- (d) **Go does not know it**: typos (`sync_interval`), keys from other versions, anything else. Go ignores these silently (`main.go:529`).

**Option A, Go's file as is (lenient).** Every file Go starts with, Rust starts with. Class (d) is ignored, as Go does. Class (b) cannot be ignored without being silently wrong: a `type: gs` replica ignored is a database with no backup, `heartbeat-url` ignored is a dead-man switch that fires, `socket.enabled` ignored breaks the operator's `litestream stop`. So even A must refuse (b) when it asks for behaviour; "as is" is not available in its pure form. What A really means is: refuse (b) when active, accept (c) and (d) silently.
- For: the widest drop-in; a Go user's file never fails on Rust over a key that did nothing on Go.
- Against: a typo still does nothing, now on both; PLAN section 3 ("unknown keys rejected") is broken for the sidecar; (c) knobs the operator set on purpose silently change meaning.

**Option B, a strict subset (refuse every unknown key).** Read with Rust's own key set; anything not in (a) fails.
- For: PLAN section 3's rule as written; nothing in the file is without effect.
- Against: "unknown" here means "unknown to v1", which includes Go's own (b) and (c) keys even when inert. A Go config with `socket: {enabled: false}`, `auto-recover: false`, `busy-timeout: 5s` or `shutdown-sync-interval: 1s`, all harmless, fails to start. Many Go configs carry such lines, so the drop-in goal fails for the common case, and the error teaches nothing the operator can act on except deleting lines Go needs if they ever roll back.

**Option C (recommended): Go's format, strict on meaning.** (a) honoured. (b) accepted only at its inert value (`enabled: false`, `false`, empty, `type: s3` or `file`), else an error naming the key path and "not in v1". (c) accepted, with one warning line per key at start saying it has no effect and why. (d) refused with the key path, and the nearest Go key when one is within edit distance 2 (`sync_interval`: did you mean `sync-interval`?).
- For: every file Go starts with either starts on Rust with the same meaning or fails at load naming the line; nothing is silently dropped that changes what is backed up; (d) refused is PLAN section 3's rule and costs only lines that never did anything on Go.
- Against: a Go file with a typo or a stale key does not start on Rust until the line is removed. That is a loud failure at deploy time, before any database is touched, and removing the line leaves the file valid for Go. `lsr config check <file>` (task 1) shows all of it before the switch.

#### O2: how health is exposed

- **Option A (recommended): the `addr` listener Go already has.** `/metrics` serves Go's series names and labels where v1 has the value (task 5 lists which), so dashboards and alerts carry over, plus new series under a distinct prefix (working name `lsr_`) for D7's states. `/healthz` returns JSON per database (D7 below). No `addr`, no listener, as in Go. For: no new file; one port Go users already open; Prometheus alerting works on day one. Against: a new HTTP path and new series names are surface Go does not have (a Go binary swapped back simply lacks them; nothing breaks); an HTTP server to write.
- **Option B: a status file** (for example `<meta>/health.json`, rewritten each tick). For: no listener. Against: a file Go does not speak (D6), in the meta directory Go reads; a write per tick per database; readers race the rename.
- **Option C: logs only.** For: nothing new. Against: D7 asks for health output; a log line is not a probe.
- For A, the server: hand-written over `std::net::TcpListener` (GET only, one request per connection, bounded request size and connection count, ~200 lines, no dependency) is recommended over `tiny_http` (MIT or Apache-2.0); the PR measures nothing it does not need to.

#### O3: where Rust-only settings live (D7 responses, O4's claim)

D7 makes each cause's response configurable; the sidecar needs somewhere to say it.
- **Option A (recommended): one top-level key in `litestream.yml`** (working name `litestream-rs:`), holding `responses:` (one entry per `Responses` field, `keep-trying` or `stop`) and O4's claim. Go ignores unknown keys (`main.go:529`), so the same file still starts Go: rollback is a binary swap. Absent, every default is `Responses::GO_LIKE`. For: one file, reviewed with the rest of the config, valid for both. Against: a key Go does not define, in a file Go reads; harmless to Go today, and a future Go that reads strictly would reject it (checked by task 9 against Head).
- **Option B: command-line flags.** Go's flag set errors on an unknown flag (`replicate.go:62-72`), so rollback means changing the unit file as well as the binary.
- **Option C: environment variables.** Invisible in config review; per process, not per database.

#### O4: TRUNCATE in the sidecar

Go runs TRUNCATE at `truncate-page-n` (121359 pages by default, `db.go:35`, `:916`). Ruling P3 runs TRUNCATE only on the host's word (`Readers::Idle`), and in the sidecar the host cannot give it at run time. Each TRUNCATE takes the boundary snapshot under the write lock (phase 3), so an attempt that comes back BUSY because the host has a reader costs a snapshot for nothing.
- **Option A (recommended): never by default, opt in by a static claim.** `Readers` is `Unknown`; the WAL stays bounded by SQLite's own restart after a full PASSIVE backfill. Under O3's key, `host-readers: none` is the operator's statement that the host holds no long readers; with it, `truncate-page-n` behaves as in Go. Without it, an explicit `truncate-page-n` other than 0 is refused (O1 class b), and the default divergence from Go is logged once at start. For: P3 holds; no wasted snapshots. Against: a WAL that grows under a host with a long reader is not truncated unless the operator opts in (Go would try).
- **Option B: copy Go.** Treat the sidecar as `Idle`. For: identical WAL size behaviour. Against: breaks P3; snapshots wasted on every BUSY.
- **Option C: probe.** Try TRUNCATE; on BUSY, act as `Pinned` for a backoff. Against: the probe is the expensive part.

#### O5: directory entries (`dir`, `pattern`, `recursive`, `watch`)

- **Option A: refuse `dir`.** Against: D6; directory configs are a Go feature with real users.
- **Option B (recommended): a static scan at start, `watch: true` refused in v1.** Paths derived exactly as Go (`main.go:716-850`: replica path or URL plus the relative path, meta path `.<file>-litestream` under `meta-path`). The header check (`IsSQLiteDatabase`, `main.go:891`) runs before any actor opens the file (global constraint). For: Go's layout for every database found; no runtime add or remove. Against: a database created after start is not replicated until restart (Go with `watch` would pick it up); the refusal says so.
- **Option C: all of it, `watch` included.** Needs actors started and stopped at run time and a rescan that never opens a file an actor holds (an fsnotify equivalent, `inotify` with no `unsafe`, means another dependency). Better as a follow-up issue.

#### O6: signals and reload

PLAN's phase 4 text says "signals for reload and shutdown". Go has no reload (`main_notwindows.go:22-25`).
- **Option A (recommended): Go's signals, restart as reload.** SIGINT and SIGTERM as Go (task 3). SIGHUP handled as SIGTERM, with a final sync: Go's runtime default ends the process with no drain, and a drain is obviously better. A config change is a restart; phase 3 makes a restart continue with no snapshot when the WAL is intact (D4), so reload buys little. For: Go's behaviour; no partial-reconfigure states. Against: a restart re-reads every database's tip (one ranged GET each).
- **Option B: SIGHUP reloads.** Re-read the file, diff the database set, start new actors, `final_sync` and stop removed ones, restart changed ones. For: no restart. Against: new behaviour Go does not have; a reload that half applies needs its own states and tests.


---

## Exit, mapped to tasks

| PLAN phase 4 item | Task |
| --- | --- |
| `lsr replicate` as a daemon: one process, many databases, one actor thread each | 2, 3 |
| Signals for shutdown (`final_sync` on SIGTERM within a deadline) and reload | 3 (O6) |
| `health()` exposed for the host | 5 (O2) |
| Configuration: Go's `litestream.yml` as is or a stricter subset | 1, 2 (O1, O3) |
| The pin across processes (P0; the descriptor hazard gone for the host) | 8 |
| Host checkpoints: how they are seen and what happens | 7, 8 |
| Exit: the phase 3 fault suite passes with the library in a separate process | 8 |
| Exit: the alternation test passes, Go sidecar to Rust sidecar and back | 9 |
| Exit: a Go user's config and meta directory start the Rust sidecar with no snapshot when the WAL is intact | 9 |
