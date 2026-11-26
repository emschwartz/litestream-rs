# The trial drill

`scripts/trial_drill.py` is the rehearsal that must pass before lsr replaces the Go litestream binary on a database of tens of GB replicated to a real provider (docs/decisions/D218.md). It runs the default config once, end to end, and fails on any mismatch or any budget exceeded.

## The phases

Each prints a PASS or FAIL line with its numbers; `summary.json` and the logs sit in the work directory.

1. generate: a synthetic database of the requested size (half the rows incompressible, in runs, so pages are mostly one kind; the line says the ratio) and a small one, 4 to 64 MiB, in one config that both binaries read: Litestream's own defaults for the snapshot interval and retention, the level intervals, L0 retention and sync interval, with `part-size`, `concurrency`, `busy-timeout` and `truncate-page-n` left unset unless a flag sets them. `--config-overlay FILE` merges a YAML fragment over it (top-level keys, `db:` into every database, `db.replica:` into every replica), so a shape under test is passed at run time. The writer (`trial_drill.py load`, run as `-exec`) inserts, updates and deletes 50/40/10 at 40 tx/s with SQLite's autocheckpoint off, and keeps a ledger.
2. go-first: the pinned Go build (3e0fdc0) replicates through its first snapshot under load, then stops; the drill proves the exit code and that no process of its group is left.
3. takeover: lsr starts on the same database, working folder, prefix and config: time to the first upload, whether it snapshots and why, how long the writer's lock waits and commits took.
4. lsr-load: lsr runs until compaction reaches level 3, a scheduled snapshot completes after the kills, a retention pass deletes something and `--min-run` minutes passed; kill -9 of the whole process group during a large upload, a compaction (a GET burst, since lsr reads inputs only to compact) and a snapshot (staged, or its upload in flight), each followed by a restart, catch-up and a restore of the small database judged by the ledger. Once a second: resident memory, CPU, staging and WAL sizes, the unreplicated age, bytes sent and requests by kind (`samples.jsonl`).
5. restore: lsr stops; both binaries restore the newest state, one transaction id and one instant; each is judged against the ledger and the two are compared page for page; `integrity_check` runs on lsr's restore.
6. rollback: Go starts on the same state, continues under load (the line says whether it snapshotted), stops; the restores repeat, and the transaction id must restore to the same pages as in phase 5.
7. summary: the table, `summary.json`, and the removal of the run prefix, listed empty afterwards.

The ledger: each transaction also writes `drill_state(seq, ts_ms, xor)`, the XOR of a 64-bit digest of every row. The writer appends "I seq ts xor" before COMMIT and "A seq ts" after. A restored database is judged by recomputing the XOR, finding (seq, xor) in the ledger, the instant bound, and (newest) equality with the live pages.

## Run it

- `just trial-drill <gib> [options]`: local MinIO (Docker), default intervals. `just trial-drill-small` is the nightly's 0.05 GiB run with every interval in seconds, an 8 MB part size and a truncate threshold set as stress settings (multipart uploads that stay in flight, the truncation path), and uploads limited to 4 MiB/s, so a large upload stays in flight long enough to kill. `just trial-drill-canary` checks the checkers.
- `just trial-drill-provider tigris <gib> [options]`: a real bucket (credentials as in scripts/provider.sh), one run prefix, removed with its open multipart uploads and asserted empty. Never part of CI.
- Options (see `--help`): `--snapshot-interval` (the scheduled snapshot is a kill point, so the default 24h means a 24 h wait; 3h is a fair compromise), the other config flags (`--levels`, `--part-size`, `--concurrency`, `--truncate-page-n`, ...), `--config-overlay`, `--stop-after N`, `--min-run`, `--tps`, `--budget-*`, `--work DIR`, `--go`, `--lsr`.

## A full-size run (40 GiB) needs

- Linux, a release lsr and the pinned Go build (`just trial-drill` builds both), Python 3.11 under `uv`.
- Disk: the live database, two restores at once (twice the size), the replica's staging and the WAL: about 4 times the database, plus the ledger. The page comparison with a live copy is skipped above 4 GiB (`--live-copy-max-gib`); the ledger check stands.
- Time: generation (about 50 s a GiB), Go's and lsr's first full uploads at the link's speed, at least 90 minutes of phase 4 (level 3 is the 1 h compaction), then 24 restores, each a download plus a full scan. Plan a day.
- Budgets (each a flag, a run over any fails): lag 60 s once caught up, 900 s to catch up, 120 s without an upload, writer stalls 25 s, memory 256 MiB plus 32 bytes a page, WAL 1 GiB.
