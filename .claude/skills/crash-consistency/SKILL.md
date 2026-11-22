---
name: crash-consistency
description: "The rules for making local state survive crashes and power loss, and for testing that it does: fsync, rename and directory-sync order, what a failed fsync means, a block-level crash law, write-order tables, and what \"durable\" means per filesystem. Use whenever code writes, renames, truncates, deletes or reads back a file it must trust after a restart: a WAL, journal, manifest, staging directory, checkpoint, marker file or atomic replace; \"is this write durable\", \"do we need an fsync here\", \"what if we crash between X and Y\", torn writes, EIO or ENOSPC handling, synchronous=NORMAL, sending or acknowledging data that was read from the page cache; and when designing or reviewing a recovery path or a crash test. Also /crash-consistency <path or design>. Not for network or replica faults (jepsen-review), or for building the simulator around the law (simulation-testing)."
---

# crash-consistency: durable means a successful fsync of the bytes and of the name

Everything written since the last successful fsync is a suggestion. After power loss the disk may hold any of it, none of it, or the later part without the earlier part. The filesystem promises less than the code assumes, and the promise differs per filesystem.

Sources are in `research.md` in the `jepsen-review` skill under "Crash consistency".

## 1. The crash law

Say which crash you mean. They are different faults.

- **Process crash** (kill -9, panic, OOM): every completed write, rename and unlink survives. Only in-memory state is lost.
- **Power loss or kernel crash**, for each file since its last successful fsync:
  - each block (think at both 512 and 4096 bytes) independently holds **any version it held** since that fsync;
  - the **length** is drawn separately: shorter than written, or extended with zeros or stale bytes on some configurations;
  - each **directory operation** (create, rename, unlink) since the last fsync of that directory independently happened or did not.
- **Lying disk**: fsync returned success and the bytes are gone anyway. State it as an assumption with what happens when it is false. On macOS plain `fsync` does not flush the drive; use `F_FULLFSYNC`.

A model in which only a prefix of the unsynced tail survives is wrong for any file that is overwritten in place, and logs that are reused or preallocated are overwritten in place as their steady state. The unit is the block, never the record: a record of header plus page straddles blocks, so "new header, old body" is an ordinary outcome.

## 2. The rules

1. **Atomic replace**: write the temp file, fsync it, rename over the target, fsync the directory. Durable when the directory fsync returns, not before.
2. **A new file exists only after its directory is synced. A deleted file stays deleted only after its directory is synced.** An unlink without it can come back after power loss.
3. **Appends are not atomic.** Checksum every record, and make the checksum cover everything the reader will trust: length, position, the identity of what came before. A checksum over the decoded meaning does not certify the stored bytes; one file's checksum does not certify the chain.
4. **A failed fsync is final.** On Linux the error is reported once and the pages are marked clean, so a retry can return success for data that never reached the disk. Never retry a bare fsync. Rewrite the data and sync again, or treat everything since the last good sync as lost. PostgreSQL panics; that is a reasonable default.
5. **A failed fsync is one-shot evidence.** A boolean in memory forgets it at the next restart. Record the debt durably before doing anything else, or stop.
6. **No durability inversion.** Never ship, acknowledge or durably stage data that its source has not made durable. Bytes read through the page cache prove visibility, not durability. Sync the source first (any descriptor on the inode), or accept the fork and name it as a reported anomaly. Example: SQLite in WAL mode with `synchronous=NORMAL` does not sync the WAL at commit, so a replicator that uploads those frames holds history the database can lose.
7. **Trust nothing past the owner's stated end.** A reused file holds valid-looking leftovers from its last life. One authority says where valid data ends, and every reader goes through it.
8. **Recovery is itself crashed into.** Each recovery step is idempotent, and each crash point inside recovery has a row in the table below.
9. **A missing record is an event, not a zero.** "No marker file" must not mean "nothing happened" when the marker can be lost with the same crash that made it necessary.
10. **Leave room for the write that matters.** ENOSPC is the likeliest cause of both the failure and the failure to record it. Preallocate the block the debt record will need.

## 3. Designing a durable record

Rules that made a design survive four review rounds:

- Every field is a **lower bound, a debt, a count or a self-validating proof**. A stale lower bound is still true. A stale debt is paid again. A stale count costs a bounded wait. A proof names bytes and is void when they differ. Nothing else is safe to read back at an unknown age.
- **An older copy of the record must always be sound.** Write the table: field, what an older value means, the consequence.
- **The worst record is no record.** Missing, unreadable, from a newer version, written for another destination, or unwritable: each is a typed event, and the system then does what it did before the record existed. Nothing is withheld because the record is broken.
- One block in a new inode per replace, so there is no torn-record case to argue.
- A proof is never more durable than its evidence: write it after the fsync of what it names.

## 4. The two tables every design carries

**Write order.** One row per pair of things that must both happen.

| Action | Record write | Order | Crash between the two | Why that is safe |
|---|---|---|---|---|
| Upload a file | advance the floor | after the upload is confirmed | object exists, floor is behind | boot sees "behind" and compares; the reverse order would claim bytes the remote never held |

**Crash points.** One row per point in each multi-step write, for both crash kinds.

| Crash point | What the disk can hold under the law | Why recovery is correct |
|---|---|---|

Fill both before writing code. A row you cannot fill is the bug.

## 5. What "durable" means per filesystem

Treat this as orientation, then test on what you ship on. The reference table is ALICE's Table 1 (2014 kernels). The length behaviours below are recalled, not re-measured; `research.md` says which rows have a source.

| Filesystem | What bites |
|---|---|
| ext4, data=ordered (default) | New blocks reach disk before the metadata commit. Overwrites of existing blocks have no order between them. Delayed allocation can persist an append after later operations, with a special case for append-then-rename |
| ext4 or ext3, data=writeback | Length can be extended over garbage |
| XFS | Appends usually crash to a shorter file. No `errors=remount-ro` equivalent, which is how fsyncgate was found |
| btrfs | Reorders directory operations freely; most ALICE vulnerabilities showed here |
| APFS, HFS+ | `fsync` does not flush the device |
| NFS, virtiofs, FUSE and gRPC-FUSE bind mounts | File locks and shared mmap may not work. Anything built on them needs a startup self-test or a documented refusal |
| Container ephemeral disk | Lost at every restart. A steady state, with a cost per start |

## 6. Testing, cheapest first

1. **Op-trace checker (unit tier).** Route every file operation through one type. Record the trace and assert the rules: nothing acted on before the fsync that makes it true, no fsync asked again after it failed.
2. **Block-subset law in a VFS shim** under the real engine and the real dependency. Let the real library judge recovery (open the crash image with real SQLite), never a second hand-written recovery.
3. **LazyFS** under the real binary: drops unsynced page cache, tears writes. It finds fsyncs the shim cannot see because the code calls the OS directly. It models a page cache, so it also only imagines; on our rig it could not see missing directory syncs.
4. **errno injection** (CharybdeFS or a FUSE shim): fsync returns EIO once, then success. ENOSPC. EROFS.
5. **Real filesystems**: dm-log-writes or dm-flakey on ext4 and XFS, weekly, if a root runner exists.

For each rig, **plant the bug it should catch**: remove each fsync, each directory sync, each ordering, one at a time. A plant no rig kills is a listed gap with a name.

## Review checklist

- [ ] Every write, rename and unlink that matters is followed by the sync that makes it true, before anything depends on it.
- [ ] Every fsync result is checked, and a failure is never followed by a bare retry.
- [ ] Nothing leaves the machine or is acknowledged before its source is synced.
- [ ] Every reader of a reused file is bounded by the owner's end.
- [ ] Every checksum's coverage is written down next to what readers trust.
- [ ] Both tables exist and have no empty cells.
- [ ] The test's crash model keeps block subsets, draws length separately, and loses directory entries.
- [ ] Recovery after each fault is tested with a restart immediately after the fault.

## Anti-patterns we hit

- A simulator whose disk could only lose a prefix, so the bug shape (a middle frame lost, later frames kept and still chained) could not be drawn.
- A crash law stated per frame when the disk works per block.
- "What the database recovers" computed by the parser under test.
- A staged-and-fsynced copy of frames the database itself had not synced: the replica was more durable than the source.
- A fsync-failed flag as a struct field, initialised false in every new process.
- A marker file whose absence meant "trusted", written after the failing tick, on the failing disk.
- A crash rig that reset its disk every 100 cycles "so disk use stays bounded", with no size assertion. It was hiding the bug it had just found.
- A regression test for a crash loop made of eight idle boots.

## Done

Both tables are filled. Every rule in section 2 is either followed or listed as an accepted limit with an owner. Each fsync and ordering has a plant that some rig kills, or is a named gap. The assumptions (honest disk, working locks, which filesystems) are written where users will read them.
