# Beyond Go: state and metadata Go does not speak

Status: proposal, parked until after v1. Written 2026-11-20.

## Why this is parked

v1 copies Go's behaviour unless a better behaviour is obvious and needs nothing Go does not speak (decision D6 in `docs/PLAN.md`). The replica and the meta directory stay exactly Go's (D2, D4): any Go version from v0.5.16 can take over a Rust replica, and Rust can take over Go's, at any moment, with a restart.

Several gaps found while planning phase 3 can only be closed by state Go has no place for. Each one alone would add its own file, header or object. This proposal collects them so they can be decided together, with one design, instead of growing a file at a time.

## The gaps

| # | Gap | Today (v1) | What closes it | Tracked |
| --- | --- | --- | --- | --- |
| G1 | A power loss drops two or more replicated transactions, and the WAL regrows with a byte-identical tip at the same offset. Boot's whole-tip compare (phase 2 V3) passes, and the replica's history silently differs from the database | V3, which already checks more than Go (Go compares the last page and the salts) | The WAL's cumulative frame checksum at the tip, remembered across a restart | #12 |
| G2 | A database page corrupted while the process was down is trusted on its first read after boot | Trust on first use, seeded from the staged files (phase 3 P5); Go checks nothing | The page checksum table remembered across a restart, or a check against the replica | #10 |
| G3 | A degraded state (phase 3 P1, a page that changed) does not survive a restart | Cleared by a restart; documented | A remembered marker | #10 |
| G4 | Another writer on the same prefix is seen only when a listing shows files above the local tip, and both writers' files can interleave before that | Detected at boot or after an ambiguous upload, as Go | A writer identity on every file, or a conditional PUT | #13 |
| G5 | A Go- or Rust-written chain with `NoChecksum` files gives nothing to check a restored page against except the live database | Scheduled verify-restores (PLAN section 7) | Rolling checksums in LTX headers (D2) | #14 |
| G6 | A corrupt compacted file fails a restore even when its inputs survive; nothing links a file to the file before it | Boot read-back repairs the newest file per level (phase 2 6g2) | A parent-file checksum per file, and a planner that routes around a bad file | #14 |
| G7 | The pin's read mark 0 does not block a WAL restart, so correctness there rests on ruling F1 | F1 plus its abandoned-frames check | A pure-Rust pin on a read-mark slot of 1 or more | #9 |

G7 needs no new file or format: it is listed because it is also behaviour beyond Go. Conditional PUTs (G4, second option) and rolling checksums (G5) use what S3 and LTX already define, but they change what Go would see or accept, so they belong here too.

## The proposal

### 1. One local state file, not several (G1, G2, G3)

A single file in the meta directory, beside Go's layout, for example `<meta>/litestream-rs.state`:

- **Contents:** a format version; the txid, salts and WAL offset it is current at; the WAL's cumulative frame checksum pair at that offset (G1); the page checksum table (G2), stored as a dense array of 8 bytes per page, 2 MiB for a 1 GiB database at 4 KiB pages; any degraded markers with their page numbers (G3); and a checksum over the whole file.
- **Writes:** written to a scratch name and renamed after each staged file is fsynced. It is never fsynced before the staged file it describes, and it is never trusted past it.
- **Reads:** a file that is missing, unreadable, from another version, or not current at the staged tip is ignored, and boot falls back to v1's behaviour (V3 and trust on first use). That covers a meta directory inherited from Go, a crash between the staged file and the state file, and every rewind.
- **Go compatibility:** spike S5 (phase 3 plan) must first show that every supported Go version ignores an unknown file in the meta directory. Go never deletes it. After a Go run the file is stale and ignored, so a handover costs only the fallback.
- **Cost:** one extra small write and rename per staged file. The table part could be written less often (every N files, or at checkpoints) and replayed forward from the staged files.

What it closes: G1 completely while the file is current; G2 and G3 for everything except a Go run in between and a power loss that rewinds past the file.

### 2. Writer identity on replica files (G4)

Each file Rust writes carries `x-amz-meta-litestream-rs-writer: <id>` (a random id fixed per meta directory, with a generation counter) next to Go's timestamp metadata. Go ignores unknown metadata; check this per provider in the compat suite. A listing does not carry metadata (Tigris Q4), so the check rides on the reads the engine already makes, the boot read-back and the tip read. Optionally add a conditional PUT (`If-None-Match: *` on level 0 keys) where the provider supports it. Another writer's PUT to the same key then fails instead of overwriting, which is the earliest possible detection. Tigris support for conditional PUTs is to be measured.

### 3. Rolling checksums and a parent link, once Go is no longer the fallback (G5, G6)

D2 already decides the first: turn on tracked checksums once rollback to Go is no longer required, because Go's compactor rejects them. A parent-file checksum (the previous file's trailer checksum in metadata) lets verify and restore check the chain's identity and route around a corrupt compacted file. Both are replica-format changes, so they come after cutover.

### 4. Not formats, but beyond Go (G7, and checks against the replica)

- The pure-Rust pin (#9), if phase 3's fault suite finds a case F1 misses.
- An opt-in verify at boot against the replica (#10), and a low-rate scrubber of the live database against the table (PLAN "Later versions").

## Order, if accepted

1. Spike S5, which is cheap and decides whether part 1 is possible.
2. Part 1, the state file, because it closes the only silent gap v1 has (G1).
3. Part 2, writer identity, because it is safe with Go and makes the second-writer halt fire earlier.
4. Parts 3 and 4 after cutover, or when a finding calls for them.
